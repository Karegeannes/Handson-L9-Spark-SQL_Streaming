# Handson L10 README
---

**Previously completed:** Tasks 1–3  
**Current tasks:**
- **Task 4**: Basic Streaming Ingestion and Parsing
- **Task 5**: Time-Based Fare Trend Prediction

## Prerequisites

- Apache Spark with PySpark
- Python 3.x
- `training-dataset.csv` in the root folder
- `data_generator.py` running on `localhost:9999` (streams JSON ride data)

---

## How to Run

1. **Start the data generator** (in one terminal):
   ```bash
   python data_generator.py
   ```

2. **Run Task 4** (in a new terminal):
   ```bash
   python task4.py
   ```

3. **Run Task 5** (in another terminal):
   ```bash
   python task5.py
   ```

The scripts automatically train the models (if needed), and then start real-time streaming inference.

---

## Task 4: Basic Streaming Ingestion and Parsing

### Objective
Train a **Linear Regression** model offline using `distance_km` to predict `fare_amount`. Then, in real time, ingest live ride data from the socket, apply the model, and calculate the deviation (anomaly detection) between actual and predicted fare.

### Approach
- **Offline Training**:
  - Load `training-dataset.csv`
  - Cast numeric columns
  - Use `VectorAssembler` to create the `features` vector from `distance_km`
  - Train `LinearRegression` model
  - Save model to `models/fare_model`

- **Real-Time Inference**:
  - Read streaming JSON data from socket (`localhost:9999`)
  - Parse schema
  - Apply same `VectorAssembler`
  - Load saved model and generate `prediction`
  - Compute `deviation = |fare_amount - prediction|`
  - Output results to console in **append** mode

### Results

<img width="1657" height="321" alt="task4" src="https://github.com/user-attachments/assets/427a5f55-76a8-4696-ba65-a2ba4bb9dee1" />

---

## Task 5: Time-Based Fare Trend Prediction

### Objective
Perform time-based forecasting of average fare using cyclical time features. Train on aggregated 5-minute windows, then predict future average fare trends in real time.

### Approach
- **Offline Training**:
  - Load and parse `training-dataset.csv`
  - Aggregate into **5-minute windows** and compute `avg_fare`
  - Feature engineering: `hour_of_day` and `minute_of_hour` from `window.start`
  - Train `LinearRegression` on the two time features
  - Save model to `models/fare_trend_model_v2`

- **Real-Time Inference**:
  - Stream data with watermark
  - Aggregate using **5-minute sliding windows** (slide = 1 minute)
  - Apply identical feature engineering (`hour_of_day`, `minute_of_hour`)
  - Load model and predict `avg_fare` for the next window
  - Output `window_start`, `window_end`, `actual avg_fare`, and `predicted_next_avg_fare`

### Results

<img width="1273" height="311" alt="task5" src="https://github.com/user-attachments/assets/3915a502-d936-4725-b295-3e6248264922" />

---

## Summary of Results

- **Task 4** Per-ride fare prediction with deviation scoring for anomaly detection.
- **Task 5** Forecasts next average fare using time-based features

Both tasks demonstrate end-to-end real-time ML inference with Spark Structured Streaming and MLlib.
