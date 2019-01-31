import os
import settings
import pandas as pd

# Create dictionary for column headers for both
# Acquisition and Performance files

# HEADERS = {
#     "Acquisition": [
#         "id", "channel", "seller", "interest_rate", "balance", "loan_term",
#         "origination_date", "first_payment_date", "ltv", "cltv",
#         "borrower_count", "dti", "borrower_credit_score",
#         "first_time_homebuyer", "loan_purpose", "property_type", "unit_count",
#         "occupancy_status", "property_state", "zip", "insurance_percentage",
#         "product_type", "co_borrower_credit_score",
#         "mortgage_insurance_entity", "relocation_indicator"
#     ],
#     "Performance": [
#         "id", "reporting_period", "servicer_name", "interest_rate", "balance",
#         "loan_age", "months_to_maturity", "adj_months_to_maturity", "maturity_date", "msa",
#         "delinquency_status", "modification_flag", "zero_balance_code",
#         "zero_balance_date", "last_paid_installment_date", "foreclosure_date",
#         "disposition_date", "foreclosure_costs", "property_repair_costs",
#         "recovery_costs", "misc_costs", "tax_costs", "sale_proceeds",
#         "credit_enhancement_proceeds", "repurchase_proceeds",
#         "other_foreclosure_proceeds", "non_interest_bearing_balance",
#         "principal_forgiveness_balance", "repurchase_proceeds_flag",
#         "foreclosure_write-off_amount", "servicing_activity"
#     ]
# }

# Create dictionary for the columns we want to keep for both files.
# We want to discard most columns in the performance file to
# dramatically decrease the file size

HEADERS = {
    "Acquisition": {
        "id": 'str',
        "channel": 'str',
        "seller": 'str',
        "interest_rate": 'float',
        "balance": 'float',
        "loan_term": 'int',
        "origination_date": 'str',
        "first_payment_date": 'str',
        "ltv": 'float',
        "cltv": 'float',
        "borrower_count": 'float',
        "dti": 'float',
        "borrower_credit_score": 'float',
        "first_time_homebuyer": 'str',
        "loan_purpose": 'str',
        "property_type": 'str',
        "unit_count": 'int',
        "occupancy_status": 'str',
        "property_state": 'str',
        "zip": 'str',
        "insurance_percentage": 'float',
        "product_type": 'str',
        "co_borrower_credit_score": 'float',
        "mortgage_insurance_entity": 'float',
        "relocation_indicator": 'str'
    },
    "Performance": {
        "id": 'str',
        "reporting_period": 'str',
        "servicer_name": 'str',
        "interest_rate": 'float',
        "balance": 'float',
        "loan_age": 'float',
        "months_to_maturity": 'float',
        "adj_months_to_maturity": 'float',
        "maturity_date": 'str',
        "msa": 'str',
        "delinquency_status": 'str',
        "modification_flag": 'str',
        "zero_balance_code": 'str',
        "zero_balance_date": 'str',
        "last_paid_installment_date": 'str',
        "foreclosure_date": 'str',
        "disposition_date": 'str',
        "foreclosure_costs": 'float',
        "property_repair_costs": 'float',
        "recovery_costs": 'float',
        "misc_costs": 'float',
        "tax_costs": 'float',
        "sale_proceeds": 'float',
        "credit_enhancement_proceeds": 'float',
        "repurchase_proceeds": 'float',
        "other_foreclosure_proceeds": 'float',
        "non_interest_bearing_balance": 'float',
        "principal_forgiveness_balance": 'float',
        "repurchase_proceeds_flag": 'str',
        "foreclosure_write-off_amount": 'float',
        "servicing_activity": 'str'
    }
}

SELECT = {
    "Acquisition": list(HEADERS["Acquisition"].keys()),
    "Performance": ["id", "foreclosure_date"]
}
# Write function to concatenate all disparate data sets
# together into one single file with proper column names


def concatenate(prefix="Acquisition"):
    files = sorted(os.listdir(settings.DATA_DIR))
    full = []
    for f in files:
        if not f.startswith(prefix):
            continue

        data = pd.read_csv(
            os.path.join(settings.DATA_DIR, f),
            sep="|",
            header=None,
            names=list(HEADERS[prefix].keys()),
            index_col=False,
            dtype=HEADERS[prefix])
        data = data[SELECT[prefix]]
        full.append(data)

    full = pd.concat(full, axis=0)

    full.to_csv(
        os.path.join(settings.PROCESSED_DIR, "{}.txt".format(prefix)),
        sep="|",
        header=SELECT[prefix],
        index=False)


if __name__ == "__main__":
    concatenate("Acquisition")
    concatenate("Performance")
