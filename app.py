import streamlit as st
import requests

st.set_page_config(page_title="J-Quants接続診断", layout="wide")
st.title("J-Quants接続診断")

api_key = None
try:
    api_key = st.secrets["JQUANTS_API_KEY"]
except Exception:
    api_key = None

if not api_key:
    st.error("JQUANTS_API_KEY を読み込めていません")
    st.stop()

api_key = api_key.strip()

st.success("JQUANTS_API_KEY を読み込めました")
st.write("長さ:", len(api_key))
st.write("ドット数:", api_key.count("."))
st.write("先頭4文字:", api_key[:4] if len(api_key) >= 4 else api_key)
st.write("末尾4文字:", api_key[-4:] if len(api_key) >= 4 else api_key)

if st.button("接続テスト"):
    url = "https://api.jquants.com/v1/listed/info"
    headers = {"x-api-key": api_key}

    try:
        r = requests.get(url, headers=headers, timeout=20)
        st.write("HTTPステータス:", r.status_code)
        try:
            st.json(r.json())
        except Exception:
            st.text(r.text)
    except Exception as e:
        st.error(f"通信エラー: {e}")
