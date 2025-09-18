from flask import Flask, request, jsonify, render_template
import pandas as pd
import unicodedata
import re
import json
import os

# 퍼지매칭은 선택적 (설치되어 있을 경우 사용)
try:
    from rapidfuzz import process, fuzz
    RAPIDFUZZ_AVAILABLE = True
except Exception:
    RAPIDFUZZ_AVAILABLE = False

# --- START: Render 배포를 위한 파일 경로 설정 ---
# Render의 영구 디스크를 '/var/data' 경로에 마운트할 예정입니다.
DATA_DIR = os.environ.get('RENDER_DISK_PATH', '.')  # 로컬 테스트를 위해 기본값 '.' 사용
MAPPING_FILE_PATH = os.path.join(DATA_DIR, 'mapping.xlsx')
INVENTORY_FILE_PATH = os.path.join(DATA_DIR, 'inventory.xlsx')
LAST_RESULT_FILE_PATH = os.path.join(DATA_DIR, 'last_result.json')
# --- END: Render 배포를 위한 파일 경로 설정 ---

app = Flask(__name__)


def normalize_text(s):
    """
    문자열을 비교 용도로 정규화:
    - NaN -> ''
    - strip, lower
    - unicode NFKC 정규화
    - NBSP 및 제로폭 문자 제거
    """
    if pd.isna(s):
        return ''
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKC", s)
    s = s.replace('\xa0', ' ')
    # zero-width 및 방향성 제어 문자 제거
    s = re.sub(r'[\u200b-\u200f\u202a-\u202e]', '', s)
    # 여러 공백은 하나로
    s = re.sub(r'\s+', ' ', s)
    return s


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/last_result", methods=["GET"])
def get_last_result():
    try:
        with open(LAST_RESULT_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify(data)
    except FileNotFoundError:
        return jsonify({"data": None})  # 파일이 없으면 데이터가 없음을 알림
    except Exception as e:
        return jsonify({"error": f"이전 결과 로딩 실패: {e}"}), 500


@app.route("/inventory", methods=["GET", "POST"])
def handle_inventory():
    if request.method == "GET":
        try:
            df = pd.read_excel(INVENTORY_FILE_PATH, dtype=str)
            df = df.fillna('')
            return jsonify(df.to_dict(orient='records'))
        except FileNotFoundError:
            return jsonify([])
        except Exception as e:
            return jsonify({"error": f"재고 파일 로딩 실패: {e}"}), 500

    if request.method == "POST":
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "전송된 데이터가 없습니다."}), 400

            df = pd.DataFrame(data)
            df.to_excel(INVENTORY_FILE_PATH, index=False)

            return jsonify({"message": "재고가 성공적으로 저장되었습니다."})
        except Exception as e:
            return jsonify({"error": f"재고 저장 실패: {e}"}), 500

    return jsonify({"error": "허용되지 않은 요청 방식입니다."}), 405


@app.route("/mapping", methods=["GET", "POST"])
def handle_mapping():
    if request.method == "GET":
        try:
            # 저장된 매핑 파일이 있으면 읽어서 JSON으로 반환
            df = pd.read_excel(MAPPING_FILE_PATH, dtype=str)
            df = df.fillna('')  # NaN 값을 빈 문자열로 변환
            return jsonify(df.to_dict(orient='records'))
        except FileNotFoundError:
            # 파일이 없으면 빈 리스트 반환
            return jsonify([])
        except Exception as e:
            return jsonify({"error": f"매핑 파일 로딩 실패: {e}"}), 500

    if request.method == "POST":
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "전송된 데이터가 없습니다."}), 400

            # 클라이언트로부터 받은 JSON 데이터를 DataFrame으로 변환
            df = pd.DataFrame(data)

            # DataFrame을 mapping.xlsx 파일로 서버에 저장
            df.to_excel(MAPPING_FILE_PATH, index=False)

            return jsonify({"message": "상품명 변경 규칙이 성공적으로 저장되었습니다."})
        except Exception as e:
            return jsonify({"error": f"매핑 규칙 저장 실패: {e}"}), 500

    return jsonify({"error": "허용되지 않은 요청 방식입니다."}), 405


@app.route("/upload", methods=["POST"])
def upload_excel_file():
    if 'file' not in request.files:
        return jsonify({"error": "파일이 없습니다."}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "파일이 선택되지 않았습니다."}), 400

    try:
        df = pd.read_excel(file)
        # -------------------------------------------------
        # 1) full data payload 준비
        # -------------------------------------------------
        full_df = df.fillna('')
        full_header = full_df.columns.tolist()
        full_rows = full_df.values.tolist()
        full_data_payload = [full_header] + full_rows

        # -------------------------------------------------
        # 2) 특정 컬럼 추출
        # -------------------------------------------------
        required_columns = ['주문번호', '수취인명', '수취인 주소', '상품명']
        missing_columns = [
            col for col in required_columns if col not in df.columns]

        extracted_data_payload = None
        extraction_error = None

        if missing_columns:
            extraction_error = f"추출 실패 (필수 컬럼 없음): {', '.join(missing_columns)}"
        else:
            extracted_df = df[required_columns].copy().fillna('')

            # 상품명 분리: rpartition으로 마지막 '*' 기준 분리
            parts = extracted_df['상품명'].str.rpartition('*')
            extracted_df['상품명(제품)'] = parts[0].str.strip()
            extracted_df['수량'] = parts[2].str.strip()

            # 정규화된 상품명 컬럼 추가 (매칭용)
            extracted_df['상품명(제품)_clean'] = extracted_df['상품명(제품)'].apply(
                normalize_text)

            # --- START: 상품명 변경 & 품목코드 매핑 로직 ---
            try:
                mapping_df = pd.read_excel(MAPPING_FILE_PATH, dtype=str)
                source_col = '라익미 파일 상품명'
                target_col = '상품명 조절'

                # 결정할 source_series / target_series / b_col_series (품목코드)
                source_series = None
                target_series = None
                b_col_series = None  # B열(품목코드)로부터 가져올 시리즈

                # 1) 컬럼명이 존재하는 경우 우선 사용 (컬럼명 기준)
                if source_col in mapping_df.columns and target_col in mapping_df.columns:
                    source_series = mapping_df[source_col].apply(
                        normalize_text)
                    target_series = mapping_df[target_col].apply(
                        lambda x: '' if pd.isna(x) else str(x).strip())
                else:
                    # 2) 컬럼명이 없으면 A열(0)과 E열(4)로 대체 시도
                    if mapping_df.shape[1] > 4:
                        # A열 -> 원본, E열 -> 변경
                        source_series = mapping_df.iloc[:, 0].apply(
                            normalize_text)
                        target_series = mapping_df.iloc[:, 4].apply(
                            lambda x: '' if pd.isna(x) else str(x).strip())
                        print(
                            f"INFO: '{source_col}'/'{target_col}' 컬럼이 없어 A열/E열로 대체합니다.")
                    else:
                        print("INFO: 매핑 컬럼을 찾지 못했습니다. 상품명 변경은 건너뜁니다.")

                # 품목코드(B열) 시리즈 생성 로직 수정
                if '상품코드' in mapping_df.columns:
                    b_col_series = mapping_df['상품코드'].apply(
                        lambda x: '' if pd.isna(x) else str(x).strip())
                elif mapping_df.shape[1] > 1:  # 컬럼명이 없으면 B열(인덱스 1)로 대체
                    b_col_series = mapping_df.iloc[:, 1].apply(
                        lambda x: '' if pd.isna(x) else str(x).strip())

                # 이제 실제 매핑 적용
                if source_series is not None and target_series is not None:
                    if b_col_series is not None:
                        combined = list(
                            zip(source_series.tolist(), target_series.tolist(), b_col_series.tolist()))
                        combined_sorted = sorted(combined, key=lambda x: len(
                            x[0]) if x[0] is not None else 0, reverse=True)
                    else:
                        combined = list(
                            zip(source_series.tolist(), target_series.tolist()))
                        combined_sorted = sorted(combined, key=lambda x: len(
                            x[0]) if x[0] is not None else 0, reverse=True)

                    if b_col_series is not None:
                        b_col_values = []
                        for orig in extracted_df['상품명(제품)']:
                            s = normalize_text(orig)
                            found_b = ''
                            for source_norm, _, bval in combined_sorted:
                                if source_norm and (source_norm in s):
                                    found_b = bval
                                    break
                            b_col_values.append(
                                found_b if found_b is not None else '')
                        extracted_df['품목코드'] = b_col_values
                        print("INFO: 품목코드(B열) 매핑 완료.")

                    mapped_products = []
                    if b_col_series is not None:
                        pairs = [(a, b) for (a, b, _) in combined_sorted]
                    else:
                        pairs = combined_sorted

                    for orig in extracted_df['상품명(제품)']:
                        s = normalize_text(orig)
                        mapped = None
                        for source_norm, target_name in pairs:
                            if source_norm and (source_norm in s):
                                mapped = target_name if target_name not in (
                                    None, '') else orig
                                break
                        mapped_products.append(
                            mapped if mapped is not None else orig)
                    extracted_df['상품명(제품)'] = mapped_products
                    print("INFO: 상품명 매핑 완료.")

                else:
                    print("INFO: source/target 시리즈를 구성하지 못해 상품명 매핑을 건너뜁니다.")

                # (선택) 매핑되지 않은 항목 샘플 출력 - 디버깅 도움
                unmatched_mask = extracted_df['상품명(제품)_clean'].apply(lambda x: x == '' or not any(x.find(
                    src) != -1 for src in (source_series.tolist() if source_series is not None else [])))
                # 위 방법은 간단 샘플용(모든 케이스를 완벽히 잡지는 않음)
                # 실제 디버깅 시 아래 코멘트를 해제해서 확인하세요.
                # print("매핑 안된 샘플:", extracted_df.loc[unmatched_mask, '상품명(제품)'].head(20).tolist())

                # 추가 옵션: rapidfuzz 퍼지매칭 (설치되어 있으면)
                if RAPIDFUZZ_AVAILABLE:
                    # 퍼지매칭이 필요한 row만 처리 (예: 여전히 매핑이 원본 그대로 남아있는 경우)
                    choices = (source_series.tolist()
                               if source_series is not None else [])

                    def fuzzy_map_row(clean_val):
                        if not clean_val:
                            return None
                        res = process.extractOne(
                            clean_val, choices, scorer=fuzz.partial_ratio, score_cutoff=88)
                        if res:
                            # res -> (matched_choice, score, idx)
                            matched_choice = res[0]
                            # 매칭된 choice에서 target 찾아서 반환
                            try:
                                idx = choices.index(matched_choice)
                                return target_series.iloc[idx] if target_series is not None else None
                            except ValueError:
                                return None
                        return None

                    # 퍼지로 보완 매핑 (주의: 성능 문제 있을 수 있음)
                    # extracted_df['fuzzy_mapped'] = extracted_df['상품명(제품)_clean'].apply(fuzzy_map_row)
                    # 실제 사용 시 원하시면 위 주석 해제 후 로직 합치기 가능.

            except FileNotFoundError:
                print(
                    f"INFO: {MAPPING_FILE_PATH} 파일을 찾을 수 없어 상품명 변경 및 품목코드 매핑을 건너뜁니다.")
            except Exception as map_e:
                print(f"INFO: 상품명 매핑 중 오류 발생: {map_e}")
            # --- END: 상품명 변경 & 품목코드 매핑 ---

            # '품목코드'가 있으면 재고 관련 열들을 추가합니다.
            # --- START: 재고 정보 자동 매핑 로직 ---
            if '품목코드' in extracted_df.columns:
                try:
                    inventory_df = pd.read_excel(
                        INVENTORY_FILE_PATH, dtype=str)
                    inventory_cols_to_merge = [
                        '상품코드', '본사창고', '업체창고', '스마트 인천창고', '인천창고']

                    # 재고 파일에 필요한 컬럼만 선택하고, 품목코드를 기준으로 중복 제거
                    inventory_df_subset = inventory_df[[
                        col for col in inventory_cols_to_merge if col in inventory_df.columns]].copy()
                    if '상품코드' in inventory_df_subset.columns:
                        inventory_df_subset.rename(
                            columns={'상품코드': '품목코드'}, inplace=True)
                        inventory_df_subset = inventory_df_subset.drop_duplicates(
                            subset=['품목코드'], keep='first')

                        # extracted_df와 재고 데이터를 '품목코드' 기준으로 병합
                        extracted_df = pd.merge(
                            extracted_df, inventory_df_subset, on='품목코드', how='left')
                        print("INFO: 저장된 재고 정보 자동 매핑 완료.")

                except FileNotFoundError:
                    print("INFO: inventory.xlsx 파일을 찾을 수 없어 재고 자동 매핑을 건너뜁니다.")
                except Exception as inv_e:
                    print(f"INFO: 재고 자동 매핑 중 오류 발생: {inv_e}")

                # 병합 후에도 존재하지 않거나 NaN인 재고 컬럼들을 빈 문자열로 채움
                for col in ['본사창고', '업체창고', '스마트 인천창고', '인천창고']:
                    if col not in extracted_df.columns:
                        extracted_df[col] = ''
                    extracted_df[col] = extracted_df[col].fillna('')
            # --- END: 재고 정보 자동 매핑 로직 ---

            # 재정렬 컬럼 생성 (원래 요구한 형식)
            final_columns = []
            for col in required_columns:
                if col == '상품명':
                    final_columns.extend(['상품명(제품)', '수량'])
                    if '품목코드' in extracted_df.columns:
                        final_columns.extend(
                            ['본사창고', '업체창고', '스마트 인천창고', '인천창고'])
                        final_columns.append('품목코드')
                else:
                    final_columns.append(col)

            # 존재하지 않는 컬럼이 있으면 무시하고 있는 컬럼만 선택
            final_columns = [
                c for c in final_columns if c in extracted_df.columns]
            extracted_df = extracted_df[final_columns]

            extracted_header = extracted_df.columns.tolist()
            extracted_rows = extracted_df.values.tolist()
            extracted_data_payload = [extracted_header] + extracted_rows

        # -------------------------------------------------
        # 3) 응답 구성
        # -------------------------------------------------
        response_data = {
            "full_data": full_data_payload,
            "extracted_data": extracted_data_payload,
            "extraction_error": extraction_error
        }

        # 마지막 결과를 파일에 저장
        try:
            with open(LAST_RESULT_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(response_data, f, ensure_ascii=False, indent=4)
        except Exception as save_e:
            print(f"ERROR: 마지막 결과 저장 실패: {save_e}")

        return jsonify(response_data)

    except Exception as e:
        return jsonify({"error": f"파일 처리 중 오류 발생: {e}"}), 500
