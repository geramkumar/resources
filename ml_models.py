Machine Learning (ML) is a way to make computers learn from data instead of writing every rule manually.

Traditional programming → You write all rules.
Machine Learning → You give examples/data, and the system learns patterns automatically.

Types:

Supervised Learning → Has known output/label during training.
Unsupervised Learning → No output label; discovers hidden patterns.
Association Rule Mining → Specialized unsupervised learning focused on relationship discovery between items.

Supervised → Learn from answers
Unsupervised → Find hidden patterns
Association → Find item relationships
Reinforcement → Learn by rewards and mistakes

### Quick Understanding

* **Supervised Learning** → Has known output/label during training.
* **Unsupervised Learning** → No output label; discovers hidden patterns.
* **Association Rule Mining** → Specialized unsupervised learning focused on relationship discovery between items.

| Algorithm                             | ML Type                                         | Why This Type                                                                                                                                                            | Most Common Real-World Use Cases                                                                                                               |
| ------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| **Linear Regression**                 | Supervised Learning                             | Uses labeled historical data where input features and target output are already known. Model learns relationship between variables to predict continuous numeric values. | House price prediction, sales forecasting, revenue estimation, demand forecasting, insurance premium prediction, stock trend estimation        |
| **Decision Tree**                     | Supervised Learning                             | Requires labeled training data. Learns decision rules by splitting data into branches using feature conditions.                                                          | Loan approval, fraud detection, customer churn prediction, medical diagnosis, risk scoring, claim approval systems                             |
| **Random Forest**                     | Supervised Learning                             | Uses labeled data and combines multiple decision trees to improve prediction accuracy and reduce overfitting.                                                            | Credit risk prediction, fraud detection, disease prediction, customer segmentation scoring, recommendation engines, insurance analytics        |
| **K-Means Clustering**                | Unsupervised Learning                           | Works without labeled output. Groups similar records based on distance and similarity patterns.                                                                          | Customer segmentation, market segmentation, anomaly detection, user behavior grouping, property segmentation, image compression                |
| **Frequent Pattern Growth algorithm** | Unsupervised Learning (Association Rule Mining) | Does not require labeled output. Finds hidden relationships and frequent item combinations from transaction data.                                                        | Market basket analysis, product recommendation, cross-selling, retail analytics, e-commerce recommendation systems, shopping behavior analysis |


### MODEL 1

# Enhanced House Price Prediction using Linear Regression (Real-World Style)

Below is a complete Python script that uses:

* Real-world style input data
* Structured records that can later be converted to JSON, StructType, List, or DataFrame
* Feature engineering
* Widely used ML evaluation metrics
* Clear comments written in simple English
* End-to-end workflow from raw data to prediction

---

```python
# ==============================================================
# HOUSE PRICE PREDICTION USING LINEAR REGRESSION
# REAL-WORLD STYLE MACHINE LEARNING WORKFLOW
# ============================================================== 

# -----------------------------
# 1. IMPORT REQUIRED LIBRARIES
# -----------------------------

# Pandas helps us create and manage structured datasets in table format.
# It is widely used for data cleaning, transformation, filtering, and feature preparation.
# In almost all ML projects, pandas is used as the main library for handling tabular data.

import pandas as pd

# NumPy helps perform mathematical operations efficiently.
# It is commonly used for arrays, statistics, matrix operations, and numerical calculations.
# Many ML libraries internally depend on NumPy for fast computation.

import numpy as np

# Matplotlib is used for visualization.
# It helps display charts and comparisons so we can understand prediction quality.
# Visual analysis is useful to understand model performance.

import matplotlib.pyplot as plt

# Train-test split helps divide data into learning and testing sections.
# LinearRegression is the ML algorithm used for prediction.
# Metrics help evaluate model quality after training.

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    mean_absolute_percentage_error
)

# -----------------------------
# 2. REAL-WORLD STYLE INPUT DATA
# -----------------------------

# This data is written in a real-world business format.
# Each record represents one house with multiple business fields.
# This structure can later be converted to JSON, list, Spark StructType,
# API payload, database rows, or streaming events.

house_data = [
    {
        "House_ID": "H001",
        "Location": "Pune",
        "Square_Feet": 1200,
        "Bedrooms": 2,
        "Bathrooms": 2,
        "Age_of_House": 15,
        "Parking_Spaces": 1,
        "Distance_to_City_Center": 8,
        "Price": 4200000
    },
    {
        "House_ID": "H002",
        "Location": "Pune",
        "Square_Feet": 1500,
        "Bedrooms": 3,
        "Bathrooms": 2,
        "Age_of_House": 10,
        "Parking_Spaces": 1,
        "Distance_to_City_Center": 6,
        "Price": 5500000
    },
    {
        "House_ID": "H003",
        "Location": "Pune",
        "Square_Feet": 1800,
        "Bedrooms": 3,
        "Bathrooms": 3,
        "Age_of_House": 8,
        "Parking_Spaces": 2,
        "Distance_to_City_Center": 5,
        "Price": 6800000
    },
    {
        "House_ID": "H004",
        "Location": "Pune",
        "Square_Feet": 2200,
        "Bedrooms": 4,
        "Bathrooms": 3,
        "Age_of_House": 5,
        "Parking_Spaces": 2,
        "Distance_to_City_Center": 4,
        "Price": 8200000
    },
    {
        "House_ID": "H005",
        "Location": "Pune",
        "Square_Feet": 2500,
        "Bedrooms": 4,
        "Bathrooms": 4,
        "Age_of_House": 4,
        "Parking_Spaces": 2,
        "Distance_to_City_Center": 3,
        "Price": 9100000
    },
    {
        "House_ID": "H006",
        "Location": "Pune",
        "Square_Feet": 3000,
        "Bedrooms": 5,
        "Bathrooms": 4,
        "Age_of_House": 3,
        "Parking_Spaces": 3,
        "Distance_to_City_Center": 2,
        "Price": 11500000
    }
]

# Convert raw business records into DataFrame.
# This step transforms list-based raw data into tabular format.
# ML libraries prefer structured tabular format for training.

df = pd.DataFrame(house_data)

print("\nOriginal Dataset")
print(df)

# -----------------------------
# 3. FEATURE ENGINEERING
# -----------------------------

# Feature engineering improves ML model learning.
# We create new business features from existing columns.
# These new columns help the model understand hidden relationships.

# Price per square foot is commonly used in real estate analytics.
# It helps normalize property cost relative to house size.
# This can improve feature understanding.

df['Price_Per_Sqft'] = df['Price'] / df['Square_Feet']

# Total rooms combines bedrooms and bathrooms.
# This gives a simple measure of overall house capacity.
# Sometimes combined features improve model accuracy.

df['Total_Rooms'] = df['Bedrooms'] + df['Bathrooms']

print("\nDataset After Feature Engineering")
print(df)

# -----------------------------
# 4. FEATURE SELECTION
# -----------------------------

# Feature selection identifies input columns for ML learning.
# Target column is what we want to predict.
# Here we predict house price.

X = df[[
    'Square_Feet',
    'Bedrooms',
    'Bathrooms',
    'Age_of_House',
    'Parking_Spaces',
    'Distance_to_City_Center',
    'Total_Rooms'
]]

y = df['Price']

# -----------------------------
# 5. TRAIN TEST SPLIT
# -----------------------------

# Train-test split prevents model overfitting.
# Training data teaches the model.
# Testing data evaluates how well the model performs on unseen data.

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.25,
    random_state=42
)

# -----------------------------
# 6. MODEL CREATION
# -----------------------------

# Linear Regression predicts continuous numeric values.
# It learns mathematical relationships between input variables and price.
# This is one of the most commonly used regression algorithms.

model = LinearRegression()

# -----------------------------
# 7. MODEL TRAINING
# -----------------------------

# Model training means learning patterns from historical data.
# The algorithm identifies relationships between features and house price.
# After training, it can predict future values.

model.fit(X_train, y_train)

# -----------------------------
# 8. MODEL COEFFICIENTS
# -----------------------------

# Coefficients explain feature importance.
# Positive value means price increases.
# Negative value means price decreases.

coeff_df = pd.DataFrame({
    'Feature': X.columns,
    'Coefficient': model.coef_
})

print("\nModel Coefficients")
print(coeff_df)

print("\nIntercept")
print(model.intercept_)

# -----------------------------
# 9. PREDICTION
# -----------------------------

# Prediction uses test data.
# This shows how well the model performs on unknown houses.
# ML quality depends on prediction accuracy.

y_pred = model.predict(X_test)

# -----------------------------
# 10. ADVANCED MODEL EVALUATION
# -----------------------------

# Multiple metrics are used because one metric alone is not enough.
# Each metric measures prediction quality differently.
# Real-world ML projects usually compare multiple metrics.

mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
r2 = r2_score(y_test, y_pred)
mape = mean_absolute_percentage_error(y_test, y_pred)

print("\nModel Evaluation Metrics")
print(f"MAE  : {mae}")
print(f"MSE  : {mse}")
print(f"RMSE : {rmse}")
print(f"R2 Score : {r2}")
print(f"MAPE : {mape}")

# -----------------------------
# 11. COMPARE ACTUAL VS PREDICTED
# -----------------------------

# This comparison helps understand prediction quality.
# If actual and predicted values are close, model quality is good.
# Large difference indicates poor prediction.

results = pd.DataFrame({
    'Actual_Price': y_test.values,
    'Predicted_Price': y_pred
})

print("\nActual vs Predicted")
print(results)

# -----------------------------
# 12. FUTURE HOUSE PREDICTION
# -----------------------------

# This section predicts price for a new house.
# Real-world systems use this step for online prediction.
# Business users can provide new property details.

new_house = pd.DataFrame({
    'Square_Feet': [2400],
    'Bedrooms': [4],
    'Bathrooms': [3],
    'Age_of_House': [4],
    'Parking_Spaces': [2],
    'Distance_to_City_Center': [3],
    'Total_Rooms': [7]
})

predicted_price = model.predict(new_house)

print("\nPredicted Price for New House")
print(predicted_price[0])

# -----------------------------
# 13. VISUALIZATION
# -----------------------------

# Scatter plot helps compare actual versus predicted values.
# If points stay near a straight line, prediction quality is good.
# Visual analysis improves model understanding.

plt.figure(figsize=(8, 6))

plt.scatter(y_test, y_pred)

plt.xlabel('Actual Price')
plt.ylabel('Predicted Price')
plt.title('Actual vs Predicted House Price')

plt.grid(True)

plt.show()

# ==============================================================
# END OF SCRIPT
# ==============================================================
```

---

## Top ML Evaluation Methods Used in This Script

1. **MAE (Mean Absolute Error)**

   * Measures average prediction error.
   * Lower value means better model.

2. **MSE (Mean Squared Error)**

   * Squares the error value.
   * Penalizes large mistakes.

3. **RMSE (Root Mean Squared Error)**

   * Most widely used regression metric.
   * Easier to understand because unit remains same as price.

4. **R² Score**

   * Measures how well the model explains data.
   * Closer to 1 means strong model.

5. **MAPE (Mean Absolute Percentage Error)**

   * Shows prediction error as percentage.
   * Useful for business interpretation.

---

## Common Real-World Improvements

You can later extend this script using:

* One-hot encoding for location
* Scaling using StandardScaler
* Outlier removal
* Cross validation
* Multiple algorithms comparison
* Hyperparameter tuning
* Pipeline architecture
* Model saving using pickle
* Deployment via API
* Databricks or Spark ML integration
######################################################################################################

#### MODEL 2 - DECISION TREE

# ==============================================================
# -----------------------------
# 10. ADVANCED MODEL EVALUATION
# -----------------------------

# Multiple metrics measure prediction quality.
# Each metric captures different error behavior.
# Widely used in real-world ML evaluation.

mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
r2 = r2_score(y_test, y_pred)
mape = mean_absolute_percentage_error(y_test, y_pred)

print("
Model Evaluation Metrics")
print(f"MAE  : {mae}")
print(f"MSE  : {mse}")
print(f"RMSE : {rmse}")
print(f"R2 Score : {r2}")
print(f"MAPE : {mape}")

# -----------------------------
# 11. ACTUAL VS PREDICTED
# -----------------------------

# Comparison helps understand prediction accuracy.
# Small differences indicate better performance.
# Large gaps indicate weak prediction.

results = pd.DataFrame({
    'Actual_Price': y_test.values,
    'Predicted_Price': y_pred
})

print("
Actual vs Predicted")
print(results)

# -----------------------------
# 12. FUTURE HOUSE PREDICTION
# -----------------------------

# New property information is given.
# Model predicts house price.
# This simulates a real business prediction system.

new_house = pd.DataFrame({
    'Square_Feet': [2400],
    'Bedrooms': [4],
    'Bathrooms': [3],
    'Age_of_House': [4],
    'Parking_Spaces': [2],
    'Distance_to_City_Center': [3],
    'Total_Rooms': [7]
})

predicted_price = model.predict(new_house)

print("
Predicted Price for New House")
print(predicted_price[0])

# -----------------------------
# 13. VISUALIZATION
# -----------------------------

# Scatter plot compares prediction quality.
# Points near diagonal indicate strong model accuracy.
# Visualization improves understanding.

plt.figure(figsize=(8, 6))

plt.scatter(y_test, y_pred)

plt.xlabel('Actual Price')
plt.ylabel('Predicted Price')
plt.title('Decision Tree: Actual vs Predicted House Price')

plt.grid(True)

plt.show()

# ==============================================================
# END OF SCRIPT
# ==============================================================


#### MODEL 3 - Random Forest

# ==============================================================

# Multiple metrics provide complete evaluation.
# Different metrics measure different error types.
# Widely used in real-world ML validation.

mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
r2 = r2_score(y_test, y_pred)
mape = mean_absolute_percentage_error(y_test, y_pred)

print("
Model Evaluation Metrics")
print(f"MAE  : {mae}")
print(f"MSE  : {mse}")
print(f"RMSE : {rmse}")
print(f"R2 Score : {r2}")
print(f"MAPE : {mape}")

# -----------------------------
# 11. ACTUAL VS PREDICTED
# -----------------------------

# This comparison validates prediction quality.
# Smaller gaps indicate stronger performance.
# Large gaps indicate model limitations.

results = pd.DataFrame({
    'Actual_Price': y_test.values,
    'Predicted_Price': y_pred
})

print("
Actual vs Predicted")
print(results)

# -----------------------------
# 12. FUTURE HOUSE PREDICTION
# -----------------------------

# New property information is provided.
# Model predicts expected selling price.
# This simulates real business prediction workflow.

new_house = pd.DataFrame({
    'Square_Feet': [2400],
    'Bedrooms': [4],
    'Bathrooms': [3],
    'Age_of_House': [4],
    'Parking_Spaces': [2],
    'Distance_to_City_Center': [3],
    'Total_Rooms': [7]
})

predicted_price = model.predict(new_house)

print("
Predicted Price for New House")
print(predicted_price[0])

# -----------------------------
# 13. VISUALIZATION
# -----------------------------

# Scatter plot compares predictions.
# Points near diagonal indicate stronger performance.
# Visualization improves understanding.

plt.figure(figsize=(8, 6))

plt.scatter(y_test, y_pred)

plt.xlabel('Actual Price')
plt.ylabel('Predicted Price')
plt.title('Random Forest: Actual vs Predicted House Price')

plt.grid(True)

plt.show()

# ==============================================================
# END OF SCRIPT
# ==============================================================

## MODEL 4 - K MEANS CLUSTER

# ==============================================================

# Training identifies cluster centers.
# Similar records move into same group.
# Clustering finds hidden patterns.

model.fit(X_scaled)

# -----------------------------
# 9. ASSIGN CLUSTERS
# -----------------------------

# Each house receives cluster label.
# Similar properties belong to same cluster.
# Cluster ID helps segmentation.

df['Cluster_ID'] = model.labels_

print("
Clustered Dataset")
print(df)

# -----------------------------
# 10. CLUSTER QUALITY EVALUATION
# -----------------------------

# Silhouette Score measures cluster quality.
# Higher score indicates better separation.
# Widely used for clustering evaluation.

silhouette_avg = silhouette_score(X_scaled, model.labels_)

print("
Silhouette Score")
print(silhouette_avg)

# -----------------------------
# 11. CLUSTER SUMMARY
# -----------------------------

# Cluster summary helps business understanding.
# Average values describe each segment.
# Useful for analytics and targeting.

cluster_summary = df.groupby('Cluster_ID').mean(numeric_only=True)

print("
Cluster Summary")
print(cluster_summary)

# -----------------------------
# 12. VISUALIZATION
# -----------------------------

# Scatter plot displays clusters.
# Different groups appear visually separated.
# Helps validate segmentation quality.

plt.figure(figsize=(8, 6))

plt.scatter(
    df['Square_Feet'],
    df['Price'],
    c=df['Cluster_ID']
)

plt.xlabel('Square Feet')
plt.ylabel('Price')
plt.title('K-Means Clustering: House Segmentation')

plt.grid(True)

plt.show()

# ==============================================================
# END OF SCRIPT
# ==============================================================




## MODEL 5:: FP GROWTH

# ==============================================================
Top Ranked Rules")
print(ranked_rules[[
    'antecedents',
    'consequents',
    'support',
    'confidence',
    'lift'
]].head(10))

# -----------------------------
# 10. BUSINESS INTERPRETATION
# -----------------------------

# Rules can be translated into recommendations.
# This section converts ML output into business language.
# Useful for recommendation engines.

for index, row in ranked_rules.head(5).iterrows():

    print("
Business Recommendation")

    print(
        f"Customers buying {set(row['antecedents'])} "
        f"often also buy {set(row['consequents'])}"
    )

    print(f"Support    : {round(row['support'], 2)}")
    print(f"Confidence : {round(row['confidence'], 2)}")
    print(f"Lift       : {round(row['lift'], 2)}")

# -----------------------------
# 11. VISUALIZATION
# -----------------------------

# Scatter plot compares support and confidence.
# Strong rules appear toward upper-right area.
# Visualization improves rule evaluation.

plt.figure(figsize=(8, 6))

plt.scatter(
    rules['support'],
    rules['confidence']
)

plt.xlabel('Support')
plt.ylabel('Confidence')
plt.title('FP-Growth Rule Evaluation')

plt.grid(True)

plt.show()

# -----------------------------
# 12. OPTIONAL RULE EXPORT
# -----------------------------

# Rules can be exported for reporting.
# Business users often need Excel or CSV output.
# This helps integration with analytics dashboards.

rules.to_csv('fp_growth_rules_output.csv', index=False)

print("
Rule file exported successfully")

# ==============================================================
# END OF SCRIPT
# ==============================================================



