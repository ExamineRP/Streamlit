"""
Performance UI 컨트롤러
Performance 하위 카테고리(지수분석, 섹터분석, 종목분석)를 통합 관리하는 메인 컨트롤러
"""
import streamlit as st
from performance_주요지수 import render as render_주요지수
from performance_섹터분석 import render as render_섹터분석
from performance_종목분석 import render as render_종목분석


def render():
    """Performance UI 렌더링 함수"""
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
    
    perf_tab_labels = ["지수 분석", "섹터 분석", "종목 분석"]
    if "perf_tab" not in st.session_state:
        st.session_state["perf_tab"] = "지수 분석"

    # Analysis 섹션
    with st.sidebar.expander("Analysis", expanded=True):
        sidebar_tab = st.radio(
            "Analysis",
            perf_tab_labels,
            label_visibility="collapsed",
            index=perf_tab_labels.index(st.session_state["perf_tab"]) if st.session_state["perf_tab"] in perf_tab_labels else 0,
        )
    if sidebar_tab != st.session_state.get("perf_tab"):
        st.session_state["perf_tab"] = sidebar_tab
    selected_tab = st.session_state.get("perf_tab", "지수 분석")

    st.sidebar.markdown("---")
    
    # 페이지 제목
    st.title("Analysis")
    
    # 상단 고정 탭 메뉴 (스크롤해도 항상 표시)
    st.markdown("""
    <style>
    :root {
        --kbam-sticky-top: 0.4rem;
    }
    .st-key-kbam_top_tabs {
        position: sticky !important;
        top: var(--kbam-sticky-top) !important;
        z-index: 999 !important;
        background: #ffffff !important;
        border-bottom: 1px solid #eceff4 !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
        padding: 4px 0 6px 0;
    }
    </style>
    """, unsafe_allow_html=True)
    with st.container(key="kbam_top_tabs"):
        top_tab = st.radio(
            "분석 상단 탭",
            options=perf_tab_labels,
            index=perf_tab_labels.index(selected_tab) if selected_tab in perf_tab_labels else 0,
            horizontal=True,
            label_visibility="collapsed",
            key="perf_top_nav",
        )
    if top_tab != st.session_state.get("perf_tab"):
        st.session_state["perf_tab"] = top_tab
    selected_tab = top_tab

    if selected_tab == "지수 분석":
        render_주요지수()
    elif selected_tab == "섹터 분석":
        render_섹터분석()
    else:
        render_종목분석()


# 독립 실행 시 (performance_ui.py를 직접 실행할 때)
if __name__ == "__main__" or not hasattr(st.session_state, 'main_menu'):
    # 페이지 설정
    st.set_page_config(
        page_title="Index Quant",
        page_icon=None,
        layout="wide"
    )
    
    # 사이드바 헤더
    st.sidebar.markdown("### KBAM Index Quant")
    st.sidebar.markdown("---")
    
    render()