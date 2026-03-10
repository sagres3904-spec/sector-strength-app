import streamlit as st
import requests

st.set_page_config(page_title="セクター強弱チェック", layout="wide")

st.title("日本株セクター強弱チェック")

api_key = None

try:
    api_key = st.secrets["JQUANTS_API_KEY"]
except Exception:
    api_key = None

if api_key:
    st.success("J-Quants APIキーを読み込めました")
else:
    st.error("J-Quants APIキーを読み込めていません")

st.write("最小版のテストです。ここまで出れば成功です。")

if api_key:
    if st.button("J-Quants接続テスト"):
        url = "https://api.jquants.com/v1/listed/info"
        headers = {"x-api-key": api_key}

        try:
            r = requests.get(url, headers=headers, timeout=20)

            st.write("HTTPステータス:", r.status_code)

            if r.status_code == 200:
                data = r.json()

                st.success("J-Quantsへの接続に成功しました")

                if isinstance(data, dict):
                    st.write("返ってきたキー:", list(data.keys()))

                    if "info" in data:
                        items = data["info"]
                    elif "listed_info" in data:
                        items = data["listed_info"]
                    else:
                        items = []

                    st.write("取得件数:", len(items))

                    if len(items) > 0:
                        st.write("先頭3件:")
                        st.json(items[:3])
                    else:
                        st.warning("データ形式は取得できましたが、中身の件数が0です")
                else:
                    st.warning("JSONは返りましたが、想定外の形式です")
                    st.json(data)

            else:
                st.error("J-Quants接続失敗")
                try:
                    st.json(r.json())
                except Exception:
                    st.text(r.text)

        except Exception as e:
            st.error(f"通信エラー: {e}")
            