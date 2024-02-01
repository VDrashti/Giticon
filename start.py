from snowflake.snowpark import Session
import snowflake.connector
from app_data_model import SnowpatrolDataModel
import json 
import streamlit as st
from dotenv import find_dotenv
from pathlib import Path
import sys
from streamlit_extras.switch_page_button import switch_page

project_home = Path(find_dotenv()).parent
sys.path.append(str(project_home))


st.set_page_config(layout="wide",initial_sidebar_state="collapsed")

st.markdown(
    """
    <style>
    .css-1jc7ptx, .e1ewe7hr3, .viewerBadge_container__1QSob,
    .styles_viewerBadge__1yB5_, .viewerBadge_link__1S137,
    .viewerBadge_text__1JaDK {
        display: none;
    }
    </style>
    """,
    unsafe_allow_html=True
)
hide_github_icon = """
<style>
.css-1l04j3h, .st-ki.st-kg.st-kj.st-kk.st-kl.st-km.st-kn.st-ko.st-kp.st-kq.st-kr.st-ks.st-kt.st-ku.st-kv.st-kw.st-kx.st-ky.st-kz.st-la.st-lb.st-lc.st-ld.st-le.st-lf.st-lg.st-lh.st-li.st-lj.st-lk.st-ll.st-lm.st-ln.st-lo.st-lp.st-lq.st-lr.st-ls.st-lt.st-lu.st-lv.st-lw.st-lx.st-ly.st-lz.st-ma.st-mb.st-mc.st-md.st-me.st-mf.st-mg.st-mh.st-mi.st-mj.st-mk.st-ml.st-mm.st-mn.st-mo.st-mp.st-mq.st-mr.st-ms.st-mt.st-mu.st-mv.st-mw.st-mx.st-my.st-mz.st-na.st-nb.st-nc.st-nd.st-ne.st-nf.st-ng.st-nh.st-ni.st-nj.st-nk.st-nl.st-nm.st-nn.st-no.st-np.st-nq.st-nr.st-ns.st-nt.st-nu.st-nv.st-nw.st-nx.st-ny.st-nz.st-oa.st-ob.st-oc.st-od.st-oe.st-of.st-og.st-oh.st-oi.st-oj.st-ok.st-ol.st-om.st-on.st-oo.st-op.st-oq.st-or.st-os.st-ot.st-ou.st-ov.st-ow.st-ox.st-oy.st-oz.st-pa.st-pb.st-pc.st-pd.st-pe.st-pf.st-pg.st-ph.st-pi.st-pj.st-pk.st-pl.st-pm.st-pn.st-po.st-pp.st-pq.st-pr.st-ps.st-pt.st-pu.st-pv.st-pw.st-px.st-py.st-pz.st-qa.st-qb.st-qc.st-qd.st-qe.st-qf.st-qg.st-qh.st-qi.st-qj.st-qk.st-ql.st-qm.st-qn.st-qo.st-qp.st-qq.st-qr.st-qs.st-qt.st-qu.st-qv.st-qw.st-qx.st-qy.st-qz.st-ra.st-rb.st-rc.st-rd.st-re.st-rf.st-rg.st-rh.st-ri.st-rj.st-rk.st-rl.st-rm.st-rn.st-ro.st-rp.st-rq.st-rr.st-rs.st-rt.st-ru.st-rv.st-rw.st-rx.st-ry.st-rz.st-sa.st-sb.st-sc.st-sd.st-se.st-sf.st-sg.st-sh.st-si.st-sj.st-sk.st-sl.st-sm.st-sn.st-so.st-sp.st-sq.st-sr.st-ss.st-st.st-su.st-sv.st-sw.st-sx.st-sy.st-sz.st-ta.st-tb.st-tc.st-td.st-te.st-tf.st-tg.st-th.st-ti.st-tj.st-tk.st-tl.st-tm.st-tn.st-to.st-tp.st-tq.st-tr.st-ts.st-tt.st-tu.st-tv.st-tw.st-tx.st-ty.st-tz.st-ua.st-ub.st-uc.st-ud.st-ue.st-uf.st-ug.st-uh.st-ui.st-uj.st-uk.st-ul.st-um.st-un.st-uo.st-up.st-uq.st-ur.st-us.st-ut.st-uu.st-uv.st-uw.st-ux.st-uy.st-uz.st-va.st-vb.st-vc.st-vd.st-ve.st-vf.st-vg.st-vh.st-vi.st-vj.st-vk.st-vl.st-vm.st-vn.st-vo.st-vp.st-vq.st-vr.st-vs.st-vt.st-vu.st-vv.st-vw.st-vx.st-vy.st-vz.st-wa.st-wb.st-wc.st-wd.st-we.st-wf.st-wg.st-wh.st-wi.st-wj.st-wk.st-wl.st-wm.st-wn.st-wo.st-wp.st-wq.st-wr.st-ws.st-wt.st-wu.st-wv.st-ww.st-wx.st-wy.st-wz.st-xa.st-xb.st-xc.st-xd.st-xe.st-xf.st-xg.st-xh.st-xi.st-xj.st-xk.st-xl.st-xm.st-xn.st-xo.st-xp.st-xq.st-xr.st-xs.st-xt.st-xu.st-xv.st-xw.st-xx.st-xy.st-xz.st-ya.st-yb.st-yc.st-yd.st-ye.st-yf.st-yg.st-yh.st-yi.st-yj.st-yk.st-yl.st-ym.st-yn.st-yo.st-yp.st-yq.st-yr.st-ys.st-yt.st-yu.st-yv.st-yw.st-yx.st-yy.st-yz.st-za.st-zb.st-zc.st-zd.st-ze.st-zf.st-zg.st-zh.st-zi.st-zj.st-zk.st-zl.st-zm.st-zn.st-zo.st-zp.st-zq.st-zr.st-zs.st-zt.st-zu.st-zv.st-zw.st-zx.st-zy.st-zz.st-a.st-b.st-c.st-d.st-e.st-f.st-g.st-h.st-i.st-j.st-k.st-l.st-m.st-n.st-o.st-p.st-q.st-r.st-s.st-t.st-u.st-v.st-w.st-x.st-y.st-z {
    display: none !important;
}
</style>
"""
st.markdown(hide_github_icon, unsafe_allow_html=True)
def build_snowpark_session(kwargs) -> Session:
    try:
        res=Session.builder.configs({
        "account": kwargs["account"],
        "user": kwargs["username"],
        "password": kwargs["password"],
        "warehouse": kwargs.get("warehouse", ""),
        "database": kwargs.get("database", ""),
        "schema": kwargs.get("schema", ""),
        "role": kwargs.get("role", "")
            }).create() 
    except:
        st.error(":warning: Incorrect login credentials")
        res = None
    return res

def connect_to_snowflake(**kwargs):
    if 'SNOWPARK_SESSION' not in st.session_state:
        if (kwargs["account"].strip() != "") & (kwargs["username"].strip() != "") & (kwargs["password"].strip() is not None):
            SNOWPARK_SESSION=build_snowpark_session(kwargs)
            st.session_state['SNOWPARK_SESSION']=SNOWPARK_SESSION
            st.info(f":+1: Connected to {SNOWPARK_SESSION.get_current_account()} as your default role - {SNOWPARK_SESSION.get_current_role()}")
        else:
            st.error(":warning: Missing fields")

@st.cache_data
def get_available_roles_for_user():
    return st.session_state['sdm'].get_available_roles()

@st.cache_data
def get_available_databases(role):
    return st.session_state['sdm'].get_available_databases(role)

@st.cache_data
def get_available_schemas(role, db):
    return st.session_state['sdm'].get_available_schemas(role, db)

@st.cache_data
def get_available_warehouses(role):
    return st.session_state['sdm'].get_available_warehouses(role)



# Create a session state object to store app state
if 'page' not in st.session_state:
    st.session_state.page = 'login'

# Set the page layout to be wide (call this only once, at the beginning)
# st.set_page_config(layout="wide",initial_sidebar_state="collapsed")

# Define the image you want to display
image = "Image.png"
with open(project_home / 'config/creds.json', 'r') as creds_file:
    creds = json.load(creds_file)
def init_session():
    #Create a layout with two columns for the top 70% and bottom 30% of the page
    col1, col2 = st.columns([3, 1])

    # Add the image to the first column (top 70%)
    with col1:
        # Adjust the image width to fit correctly (you can adjust the width value)
        st.image(image,output_format="PNG"  ,channels="BGR") 
        st.markdown('<style>div.block-container{padding-bottom :0px; padding-right :0px; padding-top :0px;padding-left :50px; }</style>',unsafe_allow_html=True) # Set background color to transparent
        
        

    # Add your logo to the second column (top 30%)
    with col2:
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        logo = "SnowPatrol.png"  # Replace with the path to your logo
        st.image(logo)
        st.markdown('<style>div.block-container{margin-right :70px; margin-top: 50px;  }</style>',unsafe_allow_html=True) 


        # Add login credentials below the logo
        account = st.text_input("Snowflake Account Identifier**")
        username = st.text_input("Username*")
        password = st.text_input("Password*", type="password")
            # Create a custom HTML button with rounded edges and "Connect" text
        button_html = f"""
        <button style="width: 100%; height: 35px; margin-top:20px; background: linear-gradient(to right, #a02a41 0%,    #1D4077 100%); color: white; border-radius: 15px;">Connect</button>
        """
        if st.markdown(button_html, unsafe_allow_html=True):
            try:
                # Establish a connection to Snowflake using creds
                snowflake_conn = snowflake.connector.connect(
                    user=username,
                    password=password,
                    account=account,
                    warehouse=creds['warehouse'],
                    database=creds['database'],
                    schema=creds['schema']
                )

                # If the connection is successful, set a flag to indicate the connection status
                connection_successful = True
            except Exception as e:
                # If the connection fails, display an error message
                connection_successful = False

        # Based on the connection status, you can transition to the next page or display different content
        if connection_successful:
            connect_to_snowflake(account=account , username=username , password=password)
            session_sdm = SnowpatrolDataModel(st.session_state['SNOWPARK_SESSION'])
            st.session_state['sdm']=session_sdm # Placeholder for your next page logic
            switch_page("Overview")
        

        # Use custom CSS to change the color of the "Powered by Anblicks" text to sky blue
        st.markdown(
            """
            <div style="text-align: center; margin-top: 10px;">
                <p style="font-size: 14px; color: #000080;">Powered by Anblicks</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        # Next Message or Action
        
        # You can add your desired actions or messages for the next step here

if __name__ == '__main__':
    hide_streamlit_style = """

            <style>

            #MainMenu {visibility: hidden;}

            footer {visibility: hidden;}

            </style>

            """

    st.markdown(hide_streamlit_style, unsafe_allow_html=True)
    if 'SNOWPARK_SESSION' not in st.session_state:
        init_session()
    
    
    

