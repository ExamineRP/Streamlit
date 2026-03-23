import streamlit as st

# 페이지 설정
st.set_page_config(
    page_title="Quant Dashboard",
    layout="wide"
)

# 사이드바 스타일링
st.sidebar.markdown("""
    <style>
    .sidebar-menu {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    }
    .menu-section {
        margin: 15px 0;
        padding: 10px 0;
        border-bottom: 1px solid #e0e0e0;
    }
    .menu-item {
        padding: 8px 0;
        cursor: pointer;
        transition: all 0.2s;
    }
    .menu-item:hover {
        background-color: #f0f0f0;
        padding-left: 5px;
    }
    [data-testid="stSidebar"] {
        background-color: #fafafa;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] {
        background-color: white;
    }
    </style>
""", unsafe_allow_html=True)

st.sidebar.markdown("### KBAM Index Quant")
st.sidebar.markdown("---")

# 메인 메뉴 선택
if 'main_menu' not in st.session_state:
    st.session_state.main_menu = "Analysis"

main_menu = st.sidebar.radio(
    "메인 메뉴",
    ["Analysis", "Strategy"],
    key="main_menu_radio"
)

st.session_state.main_menu = main_menu

st.sidebar.markdown("---")

# 메인 메뉴에 따라 해당 UI 모듈의 render() 함수 호출
if main_menu == "Analysis":
    from performance_ui import render
    render()
elif main_menu == "Strategy":
    from strategy_ui import render
    render()