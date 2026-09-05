import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

st.set_page_config(page_title='Anomaly Detection', page_icon='🔍', layout='wide')
st.title('🔍 Real-Time Credit Card Anomaly Detection')
st.caption('Apache Kafka + Spark + MongoDB + Snowflake Pipeline')

@st.cache_resource
def get_mongo():
    from pymongo import MongoClient
    from dotenv import load_dotenv
    import os
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env'))
    mongo_uri = os.environ.get('MONGO_URI', 'mongodb://127.0.0.1:27017')
    return MongoClient(mongo_uri,
        serverSelectionTimeoutMS=3000)['credit_card_db']['transactions']

@st.cache_resource
def get_snowflake():
    from database.snowflake_writer import SnowflakeWriter
    return SnowflakeWriter()

def load_mongo(limit=200):
    try:
        docs = list(get_mongo().find({},{'_id':0}).sort('timestamp',-1).limit(limit))
        if not docs: return pd.DataFrame()
        df = pd.DataFrame(docs)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        return df
    except Exception as e:
        st.error(f'MongoDB error: {e}')
        return pd.DataFrame()

def get_mongo_stats():
    try:
        col   = get_mongo()
        total = col.count_documents({})
        anoms = col.count_documents({'rule_anomaly': True})
        agg   = list(col.aggregate([{'$group':{'_id':None,
            'total_amount':{'$sum':'$amount'}}}]))
        amt   = agg[0].get('total_amount', 0) if agg else 0
        return total, anoms, amt
    except:
        return 0, 0, 0

def get_snowflake_stats():
    try:
        sf  = get_snowflake()
        df  = sf.query("SELECT COUNT(*) as TOTAL, SUM(CASE WHEN RULE_ANOMALY THEN 1 ELSE 0 END) as ANOMALIES FROM TRANSACTIONS")
        row = df.iloc[0] if not df.empty else {}
        return int(row.get('TOTAL', 0)), int(row.get('ANOMALIES', 0))
    except:
        return 0, 0

# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.header('⚙️ Controls')
    auto_refresh = st.toggle('Auto Refresh', True)
    refresh_secs = st.slider('Refresh every (s)', 5, 60, 15)
    max_records  = st.slider('Records to load', 50, 500, 200)
    if st.button('🔄 Refresh Now'): st.rerun()
    st.divider()
    st.success('🟢 Pipeline Active')
    st.info('MongoDB → real-time\nSnowflake → every 30s')
    st.divider()
    st.markdown("### 🚨 Manual Overrides")
    if st.button("🔴 Inject Fraud Transaction", type="primary"):
        try:
            from producer.transaction_generator import generate_transaction
            from kafka import KafkaProducer
            import json
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            txn = generate_transaction()
            # Force massive anomaly
            txn['amount'] = 45000.00
            txn['merchant'] = 'Unknown DarkWeb Store'
            txn['is_online'] = True
            producer.send('transactions', value=txn, key=txn['card_id'].encode('utf-8'))
            producer.flush()
            producer.close()
            st.toast("Fraud transaction instantly placed onto Kafka!", icon="🚨")
        except Exception as e:
            st.error(f"Failed to inject: {e}")

# ── Load Data ─────────────────────────────────────────────────
df           = load_mongo(max_records)
total, anoms, total_amt = get_mongo_stats()
sf_total, sf_anoms      = get_snowflake_stats()
fraud_rate = (anoms / total * 100) if total > 0 else 0

# ── KPI Row ───────────────────────────────────────────────────
st.subheader('📊 Live Metrics')
c1, c2, c3, c4 = st.columns(4)
c1.metric('Total Transactions',  f'{total:,}')
c2.metric('Anomalies Detected',  f'{anoms:,}',
    delta=f'+{anoms}' if anoms else None, delta_color='inverse')
c3.metric('Fraud Rate',          f'{fraud_rate:.2f}%')
c4.metric('Total Volume',        f'${total_amt:,.2f}')

st.divider()

# ── Snowflake KPI Row ─────────────────────────────────────────
st.subheader('❄️ Snowflake Warehouse Stats')
s1, s2 = st.columns(2)
s1.metric('Snowflake Total Rows',    f'{sf_total:,}')
s2.metric('Snowflake Anomaly Count', f'{sf_anoms:,}')
st.divider()

# ── Tabs ──────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    '📡 Live Feed', '🚨 Anomalies', '📊 Charts', '❄️ Snowflake Query'])

with tab1:
    st.subheader('Recent Transactions (MongoDB)')
    if df.empty:
        st.info('No data yet — start the producer and Spark job.')
    else:
        cols = ['transaction_id','card_id','timestamp','amount',
                'merchant','location_city','location_country','rule_anomaly']
        show = df[[c for c in cols if c in df.columns]].copy()
        if 'rule_anomaly' in show.columns:
            show['status'] = show['rule_anomaly'].map({True:'🚨 ANOMALY', False:'✅ Normal'})
            show = show.drop(columns=['rule_anomaly'])
        if 'amount' in show.columns:
            show['amount'] = show['amount'].apply(lambda x: f'${x:,.2f}')
        st.dataframe(show, use_container_width=True, height=400)

with tab2:
    st.subheader('Anomaly Alerts')
    if df.empty or 'rule_anomaly' not in df.columns:
        st.info('No anomalies yet.')
    else:
        adf = df[df['rule_anomaly'] == True]
        if adf.empty:
            st.success('No anomalies detected!')
        else:
            st.warning(f'⚠️ {len(adf)} anomalous transactions!')
            cols2 = ['card_id','amount','merchant','location_city',
                     'location_country','anomaly_type','anomaly_score']
            show2 = adf[[c for c in cols2 if c in adf.columns]].copy()
            if 'amount' in show2.columns:
                show2['amount'] = show2['amount'].apply(lambda x: f'${x:,.2f}')
            st.dataframe(show2, use_container_width=True)

with tab3:
    if not df.empty and 'timestamp' in df.columns and 'rule_anomaly' in df.columns:
        c1, c2 = st.columns(2)
        with c1:
            df2 = df.copy()
            df2['minute'] = df2['timestamp'].dt.floor('1min')
            ts  = df2.groupby(['minute','rule_anomaly']).size().reset_index(name='count')
            fig = px.bar(ts, x='minute', y='count', color='rule_anomaly',
                color_discrete_map={True:'#dc2626', False:'#2563eb'},
                title='Transactions per Minute')
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig2 = px.histogram(df, x='amount', nbins=40, color='rule_anomaly',
                color_discrete_map={True:'#dc2626', False:'#2563eb'},
                title='Amount Distribution', log_y=True)
            st.plotly_chart(fig2, use_container_width=True)
        if 'merchant_category' in df.columns:
            anom_cats = df[df['rule_anomaly']==True]['merchant_category'].value_counts()
            if not anom_cats.empty:
                fig3 = px.pie(values=anom_cats.values, names=anom_cats.index,
                    title='Anomalies by Category')
                st.plotly_chart(fig3, use_container_width=True)

with tab4:
    st.subheader('❄️ Run Snowflake Query')
    default_sql = "SELECT CARD_ID, COUNT(*) as TXN_COUNT, SUM(AMOUNT) as TOTAL, SUM(CASE WHEN RULE_ANOMALY THEN 1 ELSE 0 END) as ANOMALIES FROM TRANSACTIONS GROUP BY CARD_ID ORDER BY ANOMALIES DESC LIMIT 20"
    sql = st.text_area('SQL Query', value=default_sql, height=100)
    if st.button('▶️ Run Query'):
        try:
            result = get_snowflake().query(sql)
            st.dataframe(result, use_container_width=True)
        except Exception as e:
            st.error(f'Query error: {e}')

st.caption(f'Last updated: {datetime.now().strftime("%H:%M:%S")} | MongoDB: {total:,} records')
if auto_refresh:
    time.sleep(refresh_secs)
    st.rerun()
