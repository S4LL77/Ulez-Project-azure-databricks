import shap
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split


def train_price_model(df):
    """
    Trains an XGBoost model to predict used car prices including ULEZ status.
    """
    # Features: age, mileage, fuel_type, engine_size, is_ulez_compliant
    # We would categorical encode fuel_type and brand

    # Placeholder for feature selection and encoding
    features = ["vehicle_age", "mileage", "engine_size", "is_ulez_compliant"]
    X = df[features]
    y = df["price"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = xgb.XGBRegressor(objective="reg:squarederror", n_estimators=100)
    model.fit(X_train, y_train)

    # Evaluate
    preds = model.predict(X_test)
    print(f"MAE: {mean_absolute_error(y_test, preds)}")
    print(f"R2 : {r2_score(y_test, preds)}")

    # SHAP for impact explanation
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_test)

    # Save model and plot if needed
    return model, shap_values


if __name__ == "__main__":
    print("Machine Learning module for Price Impact Analysis initialized.")
