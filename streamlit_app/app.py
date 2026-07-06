import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json
import os
import time


# Real metrics produced by `make train` on the ULB dataset (time-ordered split).
# If the file isn't present (e.g. the hosted demo hasn't run training), fall back
# to the numbers actually observed on the held-out test set, which are committed
# in the README so they stay honest and in sync.
_REAL_METRICS_FALLBACK = {
    "pr_auc": 0.7331,
    "roc_auc": 0.976,
    "brier_score": 0.000884,
    "decision_threshold": 0.5,
    "precision_at_threshold": 0.5811,
    "recall_at_threshold": 0.7963,
    "f1_at_threshold": 0.6719,
    "tp": 86, "fp": 62, "fn": 22, "tn": 85273,
    "n_test": 85443, "n_test_frauds": 108,
}


def load_real_metrics():
    for p in ("mlruns/real_data_metrics.json", "../mlruns/real_data_metrics.json"):
        if os.path.exists(p):
            try:
                with open(p) as f:
                    return json.load(f), True
            except Exception:
                pass
    return _REAL_METRICS_FALLBACK, False


REAL_METRICS, _METRICS_FROM_DISK = load_real_metrics()

# Page config
st.set_page_config(
    page_title="PayGuard - Real-Time Fraud Detection",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom styling
st.markdown("""
    <style>
    .main {
        padding: 0rem 0rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        color: #ff6b6b;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid rgba(255, 107, 107, 0.3);
    }
    .fraud-alert {
        background: rgba(255, 107, 107, 0.1);
        border-left: 4px solid #ff6b6b;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .safe-transaction {
        background: rgba(76, 175, 80, 0.1);
        border-left: 4px solid #4caf50;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    </style>
    """, unsafe_allow_html=True)

# ===========================
# TITLE & HEADER
# ===========================

st.markdown(f"""
    <h1 style='text-align: center; color: #ff6b6b;'>
    PayGuard - Fraud Detection on Real Card Transactions
    </h1>
    <p style='text-align: center; color: #aaa;'>
    Real ULB dataset (284,807 transactions, 492 frauds) ·
    time-ordered split · PR-AUC {REAL_METRICS['pr_auc']:.2f} · ROC-AUC {REAL_METRICS['roc_auc']:.3f}
    </p>
    """, unsafe_allow_html=True)

st.info(
    "The transaction data is **real** — the ULB Credit-Card Fraud Detection "
    "dataset (Université Libre de Bruxelles), pulled from OpenML. The model is "
    "trained and evaluated on it with a leak-free time-ordered split. "
    "The 'stream' is a **replay** of these real transactions, not live production "
    "traffic. The Simulation tab below is illustrative only.",
    icon="ℹ️",
)

st.divider()

# ===========================
# SIDEBAR - CONTROLS
# ===========================

with st.sidebar:
    st.header("⚙️ Controls")
    
    # Mode selection
    demo_mode = st.radio(
        "Select Demo Mode",
        ["📊 Real Metrics", "🎬 Simulation", "📈 Historical Analysis"],
        help="Real Metrics: the model's actual held-out scores on real data\nSimulation: scripted illustration (fake)\nHistorical: illustrative example shapes (fake)"
    )
    
    # Refresh rate
    refresh_interval = st.slider(
        "Refresh interval (seconds)",
        1, 30, 5,
        help="How often to update metrics"
    )
    
    st.divider()
    
    st.subheader("🔗 Links")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("[GitHub](https://github.com/koutilyaY/payguard-realtime-fraud)")
    with col2:
        st.markdown("[Docs](https://github.com/koutilyaY/payguard-realtime-fraud#architecture)")
    with col3:
        st.markdown("[API](https://github.com/koutilyaY/payguard-realtime-fraud#api-documentation)")
    
    st.divider()
    
    st.subheader("📋 About")
    src = "loaded from training run" if _METRICS_FROM_DISK else "README-committed values"
    st.write(f"""
    **Data:** ULB credit-card fraud (real)

    **Split:** time-ordered (train earlier, test later)

    **Model:** LightGBM, MLflow-tracked

    **Metrics shown:** {src}

    **Stream:** replay of the real transactions
    """)

# ===========================
# REAL METRICS SECTION
# ===========================

if demo_mode == "📊 Real Metrics":
    
    # KPI Cards — the ACTUAL held-out test metrics on real data.
    st.subheader("📊 Real held-out test metrics (ULB, time-ordered split)")
    m = REAL_METRICS

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric("PR-AUC", f"{m['pr_auc']:.3f}", help="Average precision — the headline metric for imbalanced fraud")
    with col2:
        st.metric("ROC-AUC", f"{m['roc_auc']:.3f}")
    with col3:
        st.metric(f"Precision @{m['decision_threshold']}", f"{m['precision_at_threshold']*100:.1f}%")
    with col4:
        st.metric(f"Recall @{m['decision_threshold']}", f"{m['recall_at_threshold']*100:.1f}%")
    with col5:
        st.metric("Brier score", f"{m['brier_score']:.4f}", help="Lower is better — calibration of predicted probabilities")
    with col6:
        st.metric("Test frauds", f"{m['tp']}/{m['n_test_frauds']} caught",
                  help=f"out of {m['n_test']:,} held-out transactions")

    st.caption(
        f"Confusion matrix at threshold {m['decision_threshold']}: "
        f"TP={m['tp']}, FP={m['fp']}, FN={m['fn']}, TN={m['tn']:,}. "
        "These are the model's actual scores on the later (unseen) portion of the "
        "real dataset — not a random split, and not fabricated."
    )

    st.divider()
    
    # Real Data Tables
    st.subheader("🗄️ Delta Lake Medallion Architecture")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 🥉 Bronze Layer")
        st.write("""
        **Raw replayed events**
        - Real ULB transactions: 284,807
        - Frauds: 492 (0.172%)
        - Schema-validated on ingest
        """)

    with col2:
        st.markdown("### 🥈 Silver Layer")
        st.write("""
        **Deduplicated & cleaned**
        - Deduped on event_id (watermarked)
        - 28 PCA features + Amount kept
        - Invalid rows routed to DLQ
        """)

    with col3:
        st.markdown("### 🥇 Gold Layer")
        st.write("""
        **Scored per transaction**
        - LightGBM P(fraud) per event
        - REVIEW / ALLOW decision
        - Latest-per-txn upsert table
        """)
    st.caption("Layer descriptions reflect the pipeline in this repo. Row counts "
               "in a live run depend on how much of the replay you let through.")

    st.divider()

    # Model Performance — real confusion matrix from the held-out test set.
    st.subheader("🧠 LightGBM performance on the real held-out test set")

    col1, col2 = st.columns(2)

    with col1:
        # Confusion matrix at the operating threshold (real numbers).
        cm_data = np.array([
            [m["tn"], m["fp"]],   # TN, FP
            [m["fn"], m["tp"]],   # FN, TP
        ])
        fig_cm = go.Figure(data=go.Heatmap(
            z=cm_data,
            x=['Predicted Normal', 'Predicted Fraud'],
            y=['Actually Normal', 'Actually Fraud'],
            text=cm_data,
            texttemplate='%{text}',
            colorscale='RdYlGn_r',
            hovertemplate='%{y}<br>%{x}<br>Count: %{z}<extra></extra>'
        ))
        fig_cm.update_layout(
            title=f"Confusion Matrix (test set: {m['n_test']:,} real txns, thr={m['decision_threshold']})",
            template='plotly_dark',
            height=400
        )
        st.plotly_chart(fig_cm, use_container_width=True)
    
    with col2:
        # ROC Curve
        # Headline metrics as a bar chart (real values).
        names = ["PR-AUC", "ROC-AUC", f"Precision@{m['decision_threshold']}",
                 f"Recall@{m['decision_threshold']}", f"F1@{m['decision_threshold']}"]
        vals = [m["pr_auc"], m["roc_auc"], m["precision_at_threshold"],
                m["recall_at_threshold"], m["f1_at_threshold"]]
        fig_bar = go.Figure(go.Bar(
            x=names, y=vals, marker_color="#ff6b6b",
            text=[f"{v:.3f}" for v in vals], textposition="outside",
        ))
        fig_bar.update_layout(
            title="Real test metrics (0–1)", yaxis_range=[0, 1.05],
            template="plotly_dark", height=400,
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    
    st.divider()
    
    # Model Metrics Table — real numbers.
    st.subheader("📊 Detailed model metrics (real, held-out test set)")
    thr = m["decision_threshold"]
    metrics_df = pd.DataFrame({
        'Metric': ['PR-AUC (average precision)', 'ROC-AUC', 'Brier score',
                   'Precision', 'Recall', 'F1-score'],
        'Value': [f"{m['pr_auc']:.4f}", f"{m['roc_auc']:.4f}", f"{m['brier_score']:.4f}",
                  f"{m['precision_at_threshold']:.4f}", f"{m['recall_at_threshold']:.4f}",
                  f"{m['f1_at_threshold']:.4f}"],
        'Threshold': ['N/A', 'N/A', 'N/A', str(thr), str(thr), str(thr)],
    })
    st.dataframe(metrics_df, use_container_width=True, hide_index=True)
    st.caption("PR-AUC is the honest headline for this 0.17%-positive problem — "
               "ROC-AUC looks high on any imbalanced set. Numbers come from a "
               "leak-free time-ordered split, so they're modest, not perfect.")

# ===========================
# SIMULATION MODE
# ===========================

elif demo_mode == "🎬 Simulation":
    
    st.subheader("🎬 Illustrative fraud-detection animation")
    st.warning("This tab is a scripted illustration with made-up example "
               "transactions — it is NOT the real model or real data. For the real "
               "numbers, use the 'Real Metrics' tab.", icon="⚠️")
    
    # Start button
    if st.button("▶️ Start Live Simulation", key="start_sim"):
        
        progress_bar = st.progress(0)
        status_container = st.empty()
        transactions_container = st.container()
        stats_container = st.container()
        
        # Transaction data
        users = [
            {"id": "user_001", "name": "Alice Chen", "score": 0.95},
            {"id": "user_002", "name": "Bob Smith", "score": 0.87},
            {"id": "user_003", "name": "Carol Davis", "score": 0.42},
            {"id": "user_004", "name": "David Lee", "score": 0.15},
        ]
        
        transactions = []
        fraud_count = 0
        total_count = 0
        
        sequence = [
            (None, "Normal transaction"),
            (None, "Normal transaction"),
            ("velocity", "⚠️  Velocity Fraud - 3 txns in 10 seconds"),
            (None, "Normal transaction"),
            ("high_value", "⚠️  High-Value Fraud - $8,500 jewelry purchase"),
            (None, "Normal transaction"),
            (None, "Normal transaction"),
            ("atm", "⚠️  ATM Fraud - $500 unexpected withdrawal"),
            (None, "Normal transaction"),
            (None, "Normal transaction"),
        ]
        
        for idx, (fraud_type, description) in enumerate(sequence):
            # Update progress
            progress_bar.progress((idx + 1) / len(sequence))
            
            # Generate transaction
            user = users[np.random.randint(0, len(users))]
            
            if fraud_type == "velocity":
                amount = np.random.randint(50, 200)
                merchant = "ONLINE_STORE"
                risk_score = 0.89
                is_fraud = True
            elif fraud_type == "high_value":
                amount = np.random.randint(5000, 15000)
                merchant = "JEWELRY_STORE"
                risk_score = 0.92
                is_fraud = True
            elif fraud_type == "atm":
                amount = 500
                merchant = "ATM_WITHDRAWAL"
                risk_score = 0.85
                is_fraud = True
            else:
                amount = np.random.randint(20, 500)
                merchants = ["GROCERY", "GAS_STATION", "CAFE", "RETAIL"]
                merchant = merchants[np.random.randint(0, len(merchants))]
                risk_score = np.random.uniform(0.02, 0.15)
                is_fraud = False
            
            total_count += 1
            if is_fraud:
                fraud_count += 1
            
            txn = {
                "id": f"txn_{total_count:06d}",
                "user": user["name"],
                "amount": f"${amount}",
                "merchant": merchant,
                "risk": f"{risk_score:.2f}",
                "decision": "🚨 FRAUD" if is_fraud else "✅ SAFE",
                "latency": f"{np.random.uniform(3.1, 6.7):.1f}ms",
                "confidence": f"{np.random.uniform(0.85, 0.99):.1%}",
                "is_fraud": is_fraud
            }
            
            transactions.append(txn)
            
            # Update status
            status_container.write(f"**Processing:** {description}")
            
            # Display transaction
            if is_fraud:
                transactions_container.markdown(
                    f"""
                    <div class='fraud-alert'>
                    <b>{txn['id']}</b> | {txn['user']} | {txn['amount']} @ {txn['merchant']}<br>
                    <b>Risk:</b> {txn['risk']} | <b>Decision:</b> {txn['decision']} | <b>Latency:</b> {txn['latency']}
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            else:
                transactions_container.markdown(
                    f"""
                    <div class='safe-transaction'>
                    <b>{txn['id']}</b> | {txn['user']} | {txn['amount']} @ {txn['merchant']}<br>
                    <b>Risk:</b> {txn['risk']} | <b>Decision:</b> {txn['decision']} | <b>Latency:</b> {txn['latency']}
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            # Update stats
            with stats_container:
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Processed", total_count)
                with col2:
                    st.metric("Fraud Detected", fraud_count)
                with col3:
                    st.metric("Fraud Rate", f"{(fraud_count/total_count)*100:.1f}%")
                with col4:
                    st.metric("P99 Latency", "6.7ms")
            
            time.sleep(2)
        
        progress_bar.progress(1.0)
        status_container.success("✅ Simulation Complete!")

# ===========================
# HISTORICAL ANALYSIS
# ===========================

elif demo_mode == "📈 Historical Analysis":
    
    st.subheader("📈 24-Hour Historical Analysis")
    st.warning("Illustrative example data — the shapes below are hand-made to show "
               "what an operations view could look like. Not measured traffic.",
               icon="⚠️")

    # Generate 24-hour data
    hours = np.arange(0, 24)
    transactions_per_hour = np.array([
        150, 142, 138, 145, 168, 195,  # 00:00 - 05:59
        220, 245, 280, 310, 350, 380,  # 06:00 - 11:59
        390, 385, 375, 360, 355, 345,  # 12:00 - 17:59
        340, 330, 310, 280, 210, 180   # 18:00 - 23:59
    ])
    
    fraud_per_hour = np.array([
        2, 1, 1, 1, 3, 4,
        5, 6, 8, 10, 12, 14,
        15, 14, 12, 11, 10, 9,
        8, 7, 5, 4, 3, 2
    ])
    
    normal_per_hour = transactions_per_hour - fraud_per_hour
    
    # Transaction volume over time
    fig_volume = go.Figure()
    
    fig_volume.add_trace(go.Bar(
        x=hours,
        y=normal_per_hour,
        name='Normal Transactions',
        marker_color='rgba(76, 175, 80, 0.7)',
        hovertemplate='Hour %{x}:00<br>Normal: %{y}<extra></extra>'
    ))
    
    fig_volume.add_trace(go.Bar(
        x=hours,
        y=fraud_per_hour,
        name='Fraud Detected',
        marker_color='rgba(255, 107, 107, 0.7)',
        hovertemplate='Hour %{x}:00<br>Fraud: %{y}<extra></extra>'
    ))
    
    fig_volume.update_layout(
        title="Transaction Volume by Hour (24-Hour)",
        xaxis_title="Hour of Day",
        yaxis_title="Number of Transactions",
        barmode='stack',
        template='plotly_dark',
        height=400,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_volume, use_container_width=True)
    
    st.divider()
    
    # Fraud detection rate over time
    fraud_rate_per_hour = (fraud_per_hour / transactions_per_hour) * 100
    
    fig_fraud_rate = go.Figure()
    
    fig_fraud_rate.add_trace(go.Scatter(
        x=hours,
        y=fraud_rate_per_hour,
        mode='lines+markers',
        name='Fraud Detection Rate',
        line=dict(color='#ff6b6b', width=3),
        marker=dict(size=8),
        fill='tozeroy',
        fillcolor='rgba(255, 107, 107, 0.1)',
        hovertemplate='Hour %{x}:00<br>Fraud Rate: %{y:.2f}%<extra></extra>'
    ))
    
    fig_fraud_rate.update_layout(
        title="Fraud Detection Rate by Hour",
        xaxis_title="Hour of Day",
        yaxis_title="Fraud Rate (%)",
        template='plotly_dark',
        height=400,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_fraud_rate, use_container_width=True)
    
    st.divider()
    
    # Summary statistics
    st.subheader("📊 24-Hour Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Total Transactions",
            f"{transactions_per_hour.sum():,}",
            help="Last 24 hours"
        )
    
    with col2:
        st.metric(
            "Total Fraud",
            f"{fraud_per_hour.sum()}",
            f"{(fraud_per_hour.sum()/transactions_per_hour.sum())*100:.2f}%"
        )
    
    with col3:
        st.metric(
            "Avg Latency",
            "4.2ms",
            "0.1ms vs yesterday"
        )
    
    with col4:
        st.metric(
            "Peak Hour",
            "11:00",
            f"{transactions_per_hour.max()} txns"
        )
    
    with col5:
        st.metric(
            "Real PR-AUC",
            f"{REAL_METRICS['pr_auc']:.3f}",
            "held-out test"
        )

# ===========================
# FOOTER
# ===========================

st.divider()

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🏗️ Architecture")
    st.write("""
    **Pipeline:**
    Kafka (replay of real txns) →
    PySpark Structured Streaming →
    Delta Lake (bronze/silver/gold) →
    LightGBM per-transaction scoring →
    Redis + Postgres + FastAPI
    """)

with col2:
    st.markdown("### 📊 What's real")
    st.write("""
    - Data: **real** ULB fraud dataset
    - Split: time-ordered (no leakage)
    - Metrics: the model's actual scores
    - Stream: a **replay** of the real data
    - Runs locally; no production traffic
    """)

with col3:
    st.markdown("### 🔗 Resources")
    st.write("""
    [GitHub Repo](https://github.com/koutilyaY/payguard-realtime-fraud)
    
    [Full Docs](https://github.com/koutilyaY/payguard-realtime-fraud#readme)
    
    [API Reference](https://github.com/koutilyaY/payguard-realtime-fraud#api-documentation)
    """)

st.markdown("""
    <hr>
    <p style='text-align: center; color: #999; font-size: 0.9rem;'>
    PayGuard • fraud detection on the real ULB dataset • streaming demo (replay), not production
    </p>
    """, unsafe_allow_html=True)