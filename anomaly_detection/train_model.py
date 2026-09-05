import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import os
import sys

# Add root project dynamically
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producer.transaction_generator import generate_transaction

def train():
    print("Generating 10,000 historical transactions for training...")
    data = [generate_transaction() for _ in range(10000)]
    df = pd.DataFrame(data)
    
    # Feature Engineering
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['hour'] = df['timestamp'].dt.hour
    df['is_online_num'] = df['is_online'].astype(int)
    
    # Select features
    features = ['amount', 'hour', 'is_online_num', 'latitude', 'longitude']
    X = df[features]
    
    # Scale Data
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Train Isolation Forest
    print("Training Isolation Forest...")
    clf = IsolationForest(n_estimators=100, max_samples='auto', contamination=0.05, random_state=42)
    clf.fit(X_scaled)
    
    # Save Model and Scaler
    model_dir = os.path.dirname(os.path.abspath(__file__))
    joblib.dump(clf, os.path.join(model_dir, 'isolation_forest_model.pkl'))
    joblib.dump(scaler, os.path.join(model_dir, 'scaler.pkl'))
    print("Model and Scaler successfully saved to anomaly_detection/")

if __name__ == '__main__':
    train()
