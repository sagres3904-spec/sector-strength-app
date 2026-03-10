import streamlit as st

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
