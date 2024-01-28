# Import python packages
import streamlit as st
import streamlit_authenticator as stauth
import numpy as np
import uuid
from snowflake.snowpark.session import _get_active_sessions
from snowflake.snowpark.types import StringType, DoubleType, IntegerType, StructField, StructType
from snowflake.snowpark import functions as F
import yaml
from yaml.loader import SafeLoader
import os

try:
    session.close()  # type: ignore
except Exception as e:
    pass

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the YAML file
credentials_path = os.path.join(script_dir, 'user_credentials.yaml')

# Check if the file exists before opening it
if os.path.exists(credentials_path):
    with open(credentials_path) as file:
        config = yaml.load(file, Loader=yaml.SafeLoader)
else:
    print(f"The file '{credentials_path}' does not exist.")


authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)

name, authentication_status, username = authenticator.login()

if authentication_status:
    authenticator.logout('Logout', 'main')
    st.write(f'Welcome *{name}*')
    # Connect to Snowflake
    conn = st.connection("snowflake")
    session = conn.session()

    st.title('ICL Sizing Input Form')

    # Form fields arranged in three columns
    col1, col2 = st.columns(2)

    # Column 1
    with col1:
        geschlecht = st.selectbox('Geschlecht', ['Male', 'Female'])
        alter = st.selectbox('Alter', [x for x in range(0, 100)])
        auge = st.selectbox('Auge', ['OS', 'OD'])
        implant_size = st.selectbox('Implantat_Größe', [x for x in np.arange(11.25, 14, 0.25)])
        ACD = st.number_input('ACD', min_value=2.0, max_value=4.0, value=3.0, step=0.001, format="%0.3f")
        ACA_nasal = st.number_input('ACA_nasal', min_value=20.0, max_value=60.0, value=35.0, step=0.001, format="%0.3f")
        ACA_temporal = st.number_input('ACA_temporal', min_value=20.0, max_value=60.0, value=35.0, step=0.001,
                                       format="%0.3f")
        AtA = st.number_input('AtA', min_value=10.0, max_value=14.0, value=12.0, step=0.001, format="%0.3f")
        ACW = st.number_input('ACW', min_value=10.0, max_value=14.0, value=11.0, step=0.001, format="%0.3f")
        ARtAR_LR = st.number_input('ARtAR_LR', min_value=0, max_value=1000, value=250, step=1)

    # Column 2
    with col2:
        StS = st.number_input('StS', min_value=10.0, max_value=14.0, value=11.0, step=0.001, format="%0.3f")
        StS_LR = st.number_input('StS_LR', min_value=0, max_value=1000, value=250, step=1)
        CBID = st.number_input('CBID', min_value=10.0, max_value=14.0, value=11.0, step=0.001, format="%0.3f")
        CBID_LR = st.number_input('CBID_LR', min_value=500, max_value=2000, value=1000, step=1)
        mPupil = st.number_input('mPupil', min_value=3.0, max_value=9.0, value=6.0, step=0.01)

        WtW_MS_39 = st.number_input('WtW_MS_39', min_value=10.0, max_value=13.0, value=11.0, step=0.01)
        WtW_IOL_Master = st.number_input('WtW_IOL_Master', min_value=10.0, max_value=13.0, value=11.0, step=0.1,
                                         format="%0.1f")
        Sphaere = st.number_input('Sphäre', min_value=-25.0, max_value=0.0, value=-3.0, step=0.01, format="%0.2f")

        Zylinder = st.number_input('Zylinder', min_value=-5.0, max_value=0.0, value=-0.5, step=0.25, format="%0.2f")
        Achse = st.number_input('Achse', min_value=0, max_value=180, value=90, step=1)

    # Customizing the Submit button inside st.form
    with st.form(key='my_form'):
        submit_button = st.form_submit_button('Submit', help='Click to submit the form')

        # Check if the form is submitted
        if submit_button:
            # Get form data
            form_data = {
                'geschlecht': geschlecht,
                'alter': alter,
                'auge': auge,
                'implant_size': implant_size,
                'ACD': ACD,
                'ACA_nasal': ACA_nasal,
                'ACA_temporal': ACA_temporal,
                'AtA': AtA,
                'ACW': ACW,
                'ARtAR_LR': ARtAR_LR,
                'StS': StS,
                'StS_LR': StS_LR,
                'CBID': CBID,
                'CBID_LR': CBID_LR,
                'mPupil': mPupil,
                'WtW_MS_39': WtW_MS_39,
                'WtW_IOL_Master': WtW_IOL_Master,
                'Sphaere': Sphaere,
                'Zylinder': Zylinder,
                'Achse': Achse
            }

            schema = StructType([
                StructField('geschlecht', StringType()),
                StructField('alter', IntegerType()),
                StructField('auge', StringType()),
                StructField('implant_size', DoubleType()),
                StructField('ACD', DoubleType()),
                StructField('ACA_nasal', DoubleType()),
                StructField('ACA_temporal', DoubleType()),
                StructField('AtA', DoubleType()),
                StructField('ACW', DoubleType()),
                StructField('ARtAR_LR', IntegerType()),
                StructField('StS', DoubleType()),
                StructField('StS_LR', IntegerType()),
                StructField('CBID', DoubleType()),
                StructField('CBID_LR', IntegerType()),
                StructField('mPupil', DoubleType()),
                StructField('WtW_MS_39', DoubleType()),
                StructField('WtW_IOL_Master', DoubleType()),
                StructField('Sphaere', DoubleType()),
                StructField('Zylinder', DoubleType()),
                StructField('Achse', IntegerType())
            ])

            df = session.createDataFrame(data=[form_data], schema=schema)


            @F.udf(session=_get_active_sessions().pop())
            def _random_id() -> str:
                id = uuid.uuid4()
                return str(id)


            # id_udf = F.udf(_random_id, return_type=StringType())
            df = df.withColumn(
                "id",
                _random_id(),
            )

            df.write.mode("append").save_as_table('app_ingress.input_data')

            df = df.withColumn(
                "sts_cbid_implS",
                (F.col("StS") + F.col("CBID")) / 2 - F.col("implant_size")
            )
            df = df.withColumn(
                "sts_cbid_lr",
                (F.col("StS_LR") + F.col("CBID_LR")) / 2
            )
            df = df.withColumn(
                "vault",
                1615.0711983535798 - 63.88600034 * F.col("AtA") - 162.8446239 * F.col(
                    "sts_cbid_implS") - 0.63335823 * F.col("sts_cbid_lr")
            )

            result_df = df.select('id', 'sts_cbid_implS', 'sts_cbid_lr', 'vault', F.current_date().alias("created_at"))
            result_df.write.mode("append").save_as_table('model_results.model_v1')

            # Display the result in the app
            st.write('Sum of values per row:')
            st.dataframe(df.select("id", "vault", F.current_date().alias("created_at")))
            st.success('Data successfully submitted to Snowflake!')

elif authentication_status == False:
    st.error('Username/password is incorrect')
elif authentication_status == None:
    st.warning('Please enter your username and password')
