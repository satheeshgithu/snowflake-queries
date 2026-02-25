import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
from datetime import datetime
import pandas as pd
import io
import streamlit as st
import pandas as pd
import json
import re




# ✅ MUST be first Streamlit command
st.set_page_config(
    page_title="Snowflake Storage Integration Manager",
    layout="wide"
)

# -------------------------------
# Session State Init
# -------------------------------

if "setup_complete" not in st.session_state:
    st.session_state.setup_complete = False

if "config_db" not in st.session_state:
    st.session_state.config_db = None

if "config_schema" not in st.session_state:
    st.session_state.config_schema = None

if "storage_integration" not in st.session_state:
    st.session_state.storage_integration = None

# if st.session_state.setup_complete:
#     with st.sidebar:
#         st.markdown("### ⚙️ Environment Settings")

#         if st.button("🔧 Modify Setup Configuration", use_container_width=True):
#             st.session_state.setup_complete = False
#             st.rerun()

#         st.markdown("---")
#         st.markdown("**Current Configuration:**")
#         st.markdown(f"💾 Database: `{st.session_state.config_db}`")
#         st.markdown(f"📁 Schema: `{st.session_state.config_schema}`")
#         st.markdown(f"📦 Integration: `{st.session_state.storage_integration}`")


# -------------------------------
# Setup Page Toggle
# -------------------------------

if not st.session_state.setup_complete:

    # st.title("🔧 Storage Integration Setup")

    st.title("❄️ Snowflake Storage Integration Manager")
    
    session = st.connection("snowflake").session()
    
    # ============================================================
    # SESSION STATE
    # ============================================================
    for key in ["details", "creation_success"]:
        if key not in st.session_state:
            st.session_state[key] = None
    

    def get_all_databases_schemas(session):
        """Fetch all existing DBs & Schemas from ACCOUNT_USAGE.SCHEMATA"""
        query = """
        SELECT DISTINCT
            catalog_name AS CATALOG_NAME,
            schema_name AS SCHEMA_NAME
        FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA
        WHERE deleted IS NULL
        ORDER BY catalog_name, schema_name
        """
        try:
            df = session.sql(query).to_pandas()
            return df
        except Exception as e:
            st.error(f"Query failed: {e}")
            return pd.DataFrame()
    
    def ensure_database(session, db_name):
        """Ensure database exists"""
        session.sql(f'CREATE DATABASE IF NOT EXISTS "{db_name}"').collect()
        st.sidebar.success(f"✅ Database `{db_name}` ready")
    
    def ensure_schema(session, db_name, schema_name):
        """Ensure schema exists"""
        session.sql(f'CREATE SCHEMA IF NOT EXISTS "{db_name}"."{schema_name}"').collect()
        st.sidebar.success(f"✅ Schema `{schema_name}` ready")


    
    def ensure_config_table(session, db_name, schema_name):
        """Ensure INTERATION_INFO config table exists with correct schema"""
        full_table = f'"{db_name}"."{schema_name}".INTERATION_INFO'
        
        # Create table with all required columns

        session.sql(f"""
            CREATE TABLE IF NOT EXISTS  {full_table} (
                NAME STRING,
                TYPE STRING,
                CATEGORY STRING,
                ENABLED STRING,
                COMMENT STRING,
                CREATED_ON  NUMBER(38,0)
            )
        """).collect()
        
        
        # Bootstrap with existing integrations if empty
        count = session.sql(f'SELECT COUNT(*) as cnt FROM {full_table}').collect()[0]['CNT']
        if count == 0:
            st.sidebar.info("📥 Bootstrapping config table...")
            # Add your bootstrap logic here
            st.sidebar.success("✅ Config table ready!")
        
        return full_table
    
    def valid_name(name):
        import re
        return bool(re.match(r"^[a-zA-Z0-9_]+$", name))
    
    # ========================================
    # Sidebar: Database & Schema Selection
    # ========================================
    st.sidebar.header("🗄️ Database & Schema Setup")
    
    mode = st.sidebar.radio("Choose Mode", ["🔹 Select Existing", "➕ Create New"], horizontal=True)
    
    CONFIG_DB = None
    CONFIG_SCHEMA = None
    FULL_CONFIG_TABLE = None
    
    
    if mode == "🔹 Select Existing":
        schemas_df = get_all_databases_schemas(session)
        
        if not schemas_df.empty:
            options = schemas_df['CATALOG_NAME'] + "." + schemas_df['SCHEMA_NAME'].astype(str)
            selected = st.sidebar.selectbox(
                "Select DB.Schema:",
                options=options,
                index=None,
                placeholder="Choose existing DB.Schema..."
            )
            
            if selected:
                CONFIG_DB, CONFIG_SCHEMA = selected.split(".", 1)
                st.sidebar.success(f"✅ **Database**: `{CONFIG_DB}`")
                st.sidebar.success(f"✅ **Schema**: `{CONFIG_SCHEMA}`")
                
                # 🔥 ADD ENSURE FUNCTIONS HERE
                if CONFIG_DB and CONFIG_SCHEMA:
                    ensure_database(session, CONFIG_DB)
                    ensure_schema(session, CONFIG_DB, CONFIG_SCHEMA)
                    FULL_CONFIG_TABLE = ensure_config_table(session, CONFIG_DB, CONFIG_SCHEMA)
                    
                    # Show table status
                    st.sidebar.markdown("**📊 Config Table Status:**")
                    count = session.sql(f'SELECT COUNT(*) FROM {FULL_CONFIG_TABLE}').collect()[0][0]
                    st.sidebar.metric("Integrations", count)
    
        else:
            st.sidebar.warning("⚠️ No schemas found")
    
    else:  # Create New
        col1, col2 = st.sidebar.columns(2)
        with col1:
            CONFIG_DB = st.text_input("Database", placeholder="CONFIG_DB")
        with col2:
            CONFIG_SCHEMA = st.text_input("Schema", placeholder="PUBLIC")
        
        if st.sidebar.button("🚀 Create", type="primary"):
            if CONFIG_DB and CONFIG_SCHEMA:
                if valid_name(CONFIG_DB) and valid_name(CONFIG_SCHEMA):
                    ensure_database(session, CONFIG_DB)
                    ensure_schema(session, CONFIG_DB, CONFIG_SCHEMA)
                    FULL_CONFIG_TABLE = ensure_config_table(session, CONFIG_DB, CONFIG_SCHEMA)
                    st.sidebar.success("✅ All created!")
                    st.rerun()
                else:
                    st.sidebar.error("❌ Letters/numbers/_ only")
            else:
                st.sidebar.warning("⚠️ Fill both fields")
    
    # ========================================
    # Use in Main App
    # ========================================
    if FULL_CONFIG_TABLE:
        st.info(f"🎯 Using config table: `{FULL_CONFIG_TABLE}`")
        
     
    
    # ============================================================
    # FIXED BOOTSTRAP CONFIG TABLE
    # ============================================================
    def bootstrap_config_table(session, database, schema):
        df = session.sql("SHOW STORAGE INTEGRATIONS").to_pandas()
        
        if df.empty:
            return
        
        df.columns = df.columns.str.replace('"','').str.upper()
        
        required = ["NAME", "TYPE", "CATEGORY", "ENABLED", "COMMENT", "CREATED_ON"]
        
        for c in required:
            if c not in df.columns:
                df[c] = None
        
        df_final = df[required].copy()
        
        # Convert to lowercase to match table schema
        df_final.columns = df_final.columns.str.lower()
        
        df_final["created_on"] = pd.to_datetime(
            df_final["created_on"],
            errors="coerce"
        )
        
        session.write_pandas(
            df_final,
            table_name="INTERATION_INFO",
            database=database,
            schema=schema,
            auto_create_table=False,
            overwrite=True,
            quote_identifiers=False  # Add this
        )
    

    
    def insert_integration_to_config(database, schema, integration_name):
    
        df = session.sql(
            f"SHOW STORAGE INTEGRATIONS LIKE '{integration_name}'"
        ).to_pandas()
    
        if df.empty:
            st.warning("SHOW returned no rows")
            return
    
        # normalize case
        df.columns = df.columns.str.replace('"','').str.upper()
    
        required = ["NAME","TYPE","CATEGORY","ENABLED","COMMENT","CREATED_ON"]
    
        # keep only columns that actually exist
        available = [c for c in required if c in df.columns]
    
        if not available:
            st.error(f"Unexpected SHOW format → columns = {df.columns.tolist()}")
            return
    
        df = df[available]
    
        # add missing columns if needed
        for c in required:
            if c not in df.columns:
                df[c] = None
    
        df = df[required]
    
        session.write_pandas(
            df,
            table_name="INTERATION_INFO",
            database=database,
            schema=schema,
            auto_create_table=False
        )
    
        st.success("Config table updated")
    
    
    
    bootstrap_config_table(session, CONFIG_DB, CONFIG_SCHEMA)


    def get_integrations():
    
        try:
            df = session.sql(
                f'SELECT NAME FROM {FULL_CONFIG_TABLE} ORDER BY CREATED_ON DESC'
            ).to_pandas()
    
    
            return df["NAME"].tolist() if not df.empty else []
    
        except Exception as e:
            st.warning(f"Select Database & Schema")
            return []
            
    def get_unique_name(session, base_name, FULL_CONFIG_TABLE):
    
        try:
            df = session.sql(
                f"SELECT NAME FROM {FULL_CONFIG_TABLE}"
            ).to_pandas()
    
            existing = set(df["NAME"].tolist())
    
        except:
            existing = set()
    
        if base_name not in existing:
            return base_name
    
        i = 1
        while True:
            new_name = f"{base_name}_{i}"
            if new_name not in existing:
                return new_name
            i += 1




    
    
    def create_storage_integration(session,name,provider,credential,locations,FULL_CONFIG_TABLE):
    
        final_name = get_unique_name(
            session,
            name,
            FULL_CONFIG_TABLE
        )

        if final_name != name:
            st.warning(f"Name already exists — created {final_name} create with another name")

    
        if provider == "S3":
            sql = f"""
            CREATE  STORAGE INTEGRATION {final_name}
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = 'S3'
            ENABLED = TRUE
            STORAGE_AWS_ROLE_ARN = '{credential}'
            STORAGE_ALLOWED_LOCATIONS = ({','.join(f"'{l}'" for l in locations)})
            """
        elif provider == "GCS":
            sql = f"""
            CREATE  STORAGE INTEGRATION {final_name}
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = 'GCS'
            ENABLED = TRUE
            STORAGE_GOOGLE_SERVICE_ACCOUNT = '{credential}'
            STORAGE_ALLOWED_LOCATIONS = ({','.join(f"'{l}'" for l in locations)})
            """
        else:
            sql = f"""
            CREATE STORAGE INTEGRATION {final_name}
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = 'AZURE'
            ENABLED = TRUE
            STORAGE_ALLOWED_LOCATIONS = ({','.join(f"'{l}'" for l in locations)})
            """

        #     # SHOW SQL STATEMENT with syntax highlighting
        # st.markdown("### 🔧 Generated SQL")
        # st.code(sql, language="sql")
        
        # # Confirm before execution
        # if st.button("🚀 Execute SQL", type="primary", use_container_width=True):
        #     try:
        #         session.sql(sql).collect()  # Execute (show() is Snowpark UI only)
        #         st.success(f"✅ Storage Integration created: `{final_name}`")
        #         st.balloons()
        #     except Exception as e:
        #         st.error(f"❌ Failed to create: {str(e)}")
        #         st.code(sql, language="sql")  # Show SQL on error too


        st.markdown("### 🔧 Generated SQL")
        st.code(sql, language="sql")
        session.sql(sql).collect()
        st.success(f"✅ Integration created: {final_name}")
    
    def describe_integration(name):
    
        try:
            if not name:
                return None
    
            df = session.sql(
                f'DESC STORAGE INTEGRATION "{name}"'
            ).to_pandas()
    
            if df.empty:
                st.warning("DESC returned no rows")
                return None
    
            # normalize column names
            df.columns = [c.strip().lower() for c in df.columns]
    
            # auto detect columns
            prop_col = next(
                (c for c in df.columns if "property" in c or c == "name"),
                None
            )
            val_col = next(
                (c for c in df.columns if "property_value" in c),
                None
            )
    
            if not prop_col or not val_col:
                st.error(f"Unexpected DESC format → {df.columns.tolist()}")
                st.dataframe(df)
                return None
    
            props = dict(zip(df[prop_col], df[val_col]))
    
            return {
                "PROVIDER": props.get("STORAGE_PROVIDER", "N/A"),
                # "STORAGE_AWS_ROLE_ARN": props.get("STORAGE_AWS_ROLE_ARN", "N/A"),
                "LOCATIONS": props.get("STORAGE_ALLOWED_LOCATIONS", "N/A"),
                "IAM_ARN": props.get("STORAGE_AWS_IAM_USER_ARN", "N/A"),
                "EXTERNAL_ID": props.get("STORAGE_AWS_EXTERNAL_ID", "N/A")
            }
    
        except Exception as e:
            st.error(f"DESC failed: {e}")
            return None
    
    
    def build_trust_policy(iam_arn, external_id):
        return {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": iam_arn},
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": external_id
                    }
                }
            }]
        }
    
    # ============================================================
    # UI
    # ============================================================
    # st.subheader("🔧 Storage Integration Manager")
    col_title, col_doc = st.columns([6,1])
    
    with col_title:  
        st.title("🔧 Storage Integration")
    
    with col_doc:
        st.link_button(
            "📘",
            "https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration"
        )
    
    
    options = ["➕ Create New"] + get_integrations()
    choice = st.selectbox("Select Integration", options)
    
    # ============================================================
    # CREATE NEW - FIXED INDENTATION
    # ============================================================
    if choice == "➕ Create New":
        col1, col2 = st.columns(2)
    
        with col1:
            name = st.text_input("Integration Name")
            provider = st.selectbox("Provider", ["S3", "GCS", "AZURE"])
    
        with col2:
            credential = st.text_input(
                "Credential",
                placeholder="arn:aws:iam::123456789012:role/my-role"
            )
    
        locations_input = st.text_area(
            "Allowed Locations (comma separated)",
            placeholder="s3://my-bucket/path/, s3://other-bucket/data/"
        )
    
        location_list = [l.strip() for l in locations_input.split(",") if l.strip()]

    
        # Validation with visual feedback
        name_ok = bool(name.strip())
        credential_ok = bool(credential.strip())
        locations_ok = len(location_list) > 0
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Name", "*" if name_ok else "x")
        col2.metric("Credential", "*" if credential_ok else "x")
        col3.metric("Locations", f"{len(location_list)} *" if locations_ok else "x")
    
        if st.button("🚀 Create Integration", type="primary"):
        
            name_clean = name.strip()
            credential_clean = credential.strip()
        
            with st.spinner("Creating integration..."):
        
                # STEP 1 — create integration
                create_storage_integration(session,name,provider,credential,location_list,FULL_CONFIG_TABLE)
        
                # STEP 2 — insert into config table
                insert_integration_to_config(
                    CONFIG_DB,
                    CONFIG_SCHEMA,
                    name
                )
        
                # STEP 3 — describe integration
                st.session_state.details = describe_integration(name)
        
            st.success("✅ Integration created and saved to config table")
            st.rerun()

     
    
    # ============================================================
    # EXISTING INTEGRATION
    # ============================================================
    else:
        col1, col2 = st.columns(2)
    
        with col1:

            if st.button("🔍 DESC Integration") and choice != "➕ Create New":
                st.session_state.details = describe_integration(choice)
                st.rerun()
    
        with col2:
            if st.button("📜 Generate Policy"):
                st.session_state.details = describe_integration(choice)
                st.rerun()
    
    # ============================================================
    # SUCCESS MESSAGE
    # ============================================================
    if st.session_state.creation_success:
        st.success("🎊 **Integration created and ready for use!** 🎊")
        if st.button("✅ Continue"):
            st.session_state.creation_success = False
            st.rerun()
    
    # ============================================================
    # DETAILS + POLICY
    # ============================================================
    if st.session_state.details:
        st.markdown("---")
        tab1, tab2 = st.tabs(["🔐 Details", "📜 Policy JSON"])
    
        with tab1:
            d = st.session_state.details
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Provider", d["PROVIDER"] or "N/A")
                st.text_area("Allowed Locations", d["LOCATIONS"] or "N/A", height=100)
            with col2:
                st.text_input("IAM User ARN", d["IAM_ARN"] or "N/A")
                st.text_input("External ID", d["EXTERNAL_ID"] or "N/A")
    
        with tab2:
            d = st.session_state.details
            if d["IAM_ARN"] and d["EXTERNAL_ID"]:
                policy = build_trust_policy(d["IAM_ARN"], d["EXTERNAL_ID"])
                st.code(json.dumps(policy, indent=4), language="json")
                st.download_button(
                    "⬇️ Download Policy",
                    json.dumps(policy, indent=4),
                    file_name=f"{choice}_trust_policy.json"
                )
            else:
                st.warning("❌ IAM details not available - only S3 integrations have policy")
  
    
    st.markdown("---")
    
    
    options = get_integrations()
    # choice = st.selectbox("Select Integration", options)
    choice = st.selectbox("Select Integration", options, key="step1_integration_select")
    
    
    if st.button("➡️ Continue to CDC Setup", type="primary"):
    
        if choice == "➕ Create New":
            st.error("Please select or create integration first")
            st.stop()
    
        st.session_state.storage_integration = choice
        st.session_state.config_db = CONFIG_DB
        st.session_state.config_schema = CONFIG_SCHEMA
        st.session_state.setup_complete = True
    
        st.rerun()
    st.stop()
    

else:

    def initialize_session_state():
        if 'cdc_configs' not in st.session_state:
            st.session_state.cdc_configs = {}
        if 'active_config' not in st.session_state:
            st.session_state.active_config = None
        if 'log_entries' not in st.session_state:
            st.session_state.log_entries = []
        # if 'setup_complete' not in st.session_state:
        #     st.session_state.setup_complete = st.session_state.get("app_step") == 2
        if 'storage_integration' not in st.session_state:
            st.session_state.storage_integration = None
        if 'config_db' not in st.session_state:
            st.session_state.config_db = None
        if 'config_schema' not in st.session_state:
            st.session_state.config_schema = None
        if 'imported_configs' not in st.session_state:
            st.session_state.imported_configs = []
    
    def log_action(action, status, message, config_name=None):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = {
            "timestamp": timestamp,
            "action": action,
            "status": status,
            "message": message,
            "config": config_name or (st.session_state.active_config if 'active_config' in st.session_state else None)
        }
        st.session_state.log_entries.append(entry)
    
    # ------------------------------------------------
    # DATABASE AND SCHEMA MANAGEMENT
    # ------------------------------------------------
    
    def check_or_create_database(session, db_name):
        try:
            db_check = session.sql(f"SHOW DATABASES LIKE '{db_name}'").collect()
            if not db_check:
                session.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}").collect()
                log_action("Database Creation", "Success", f"Database {db_name} created")
            return True
        except Exception as e:
            log_action("Database Creation", "Failed", str(e))
            raise
    
    def check_or_create_schema(session, db_name, schema_name):
        try:
            schema_check = session.sql(f"SHOW SCHEMAS LIKE '{schema_name}' IN DATABASE {db_name}").collect()
            if not schema_check:
                session.sql(f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_name}").collect()
                log_action("Schema Creation", "Success", f"Schema {db_name}.{schema_name} created")
            return True
        except Exception as e:
            log_action("Schema Creation", "Failed", str(e))
            raise
    
    def get_databases(session):
        try:
            return [db['name'] for db in session.sql("SHOW DATABASES").collect()]
        except Exception as e:
            log_action("Get Databases", "Failed", str(e))
            return []
    
    def get_schemas(session, database):
        try:
            return [s['name'] for s in session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()]
        except Exception as e:
            log_action("Get Schemas", "Failed", str(e))
            return []
          
    
    # ------------------------------------------------
    # FILE FORMAT MANAGEMENT
    # ------------------------------------------------
    
    def check_or_create_file_format(session, schema, format_name):
        try:
            ff_check = session.sql(f"""
                SELECT * FROM INFORMATION_SCHEMA.FILE_FORMATS 
                WHERE FILE_FORMAT_SCHEMA = '{schema.split('.')[-1]}' 
                AND FILE_FORMAT_NAME = '{format_name}'
            """).collect()
            
            if not ff_check:
                session.sql(f"""
                    CREATE FILE FORMAT IF NOT EXISTS {schema}.{format_name}
                    TYPE = 'PARQUET'
                    COMPRESSION = 'AUTO'
                    -- SNAPPY_COMPRESSION = TRUE
                    BINARY_AS_TEXT = FALSE
                    USE_LOGICAL_TYPE = TRUE
                    TRIM_SPACE = TRUE
                    USE_VECTORIZED_SCANNER = TRUE
                    REPLACE_INVALID_CHARACTERS = TRUE
                    NULL_IF = ('NULL', 'null', '');
                """).collect()
                log_action("File Format Creation", "Success", f"File format {schema}.{format_name} created")
            return True
        except Exception as e:
            log_action("File Format Creation", "Failed", str(e))
            raise
    def get_file_formats(session, database, schema):

        try:
            df = session.sql(f"""
                SELECT FILE_FORMAT_NAME
                FROM {database}.INFORMATION_SCHEMA.FILE_FORMATS
                WHERE FILE_FORMAT_SCHEMA = '{schema}'
                ORDER BY FILE_FORMAT_NAME
            """).to_pandas()
    
            if df.empty:
                return []
    
            return df["FILE_FORMAT_NAME"].tolist()
    
        except:
            return []

    
    # ------------------------------------------------
    # CONFIGURATION TABLE MANAGEMENT
    # ------------------------------------------------
    
    def create_config_table(session, schema):
        try:
            session.sql(f"""
                CREATE  TABLE IF NOT EXISTS {schema}.CDC_CONFIGURATIONS (
                    CONFIG_NAME STRING,
                    SOURCE_URL STRING,
                    PRIMARY_KEY STRING,
                    STAGING_SCHEMA STRING,
                    TARGET_SCHEMA STRING,
                    WAREHOUSE STRING,
                    SCHEDULE STRING,
                    HISTORICAL_COPY BOOLEAN,
                    SNOWPIPE BOOLEAN,
                    STREAM BOOLEAN,
                    TASK_ENABLED BOOLEAN,
                    FILE_FORMAT STRING,
                    DESCRIPTION STRING,
                    CREATED_AT TIMESTAMP_LTZ,
                    LAST_MODIFIED TIMESTAMP_LTZ
                )
            """).collect()
            log_action("Config Table Creation", "Success", f"Configuration table created in {schema}")
            return True
        except Exception as e:
            log_action("Config Table Creation", "Failed", str(e))
            raise
    
    def truncate_config_table(session, schema):
        """Truncate the configuration table"""
        try:
            session.sql(f"TRUNCATE TABLE IF EXISTS {schema}.CDC_CONFIGURATIONS").collect()
            log_action("Config Table Truncation", "Success", f"Configuration table {schema}.CDC_CONFIGURATIONS truncated")
            return True
        except Exception as e:
            log_action("Config Table Truncation", "Failed", str(e))
            raise
    
    def save_config_to_table(session, schema, config_name, config_data):
        try:


            historical_copy = 'TRUE' if config_data['historical_copy'] else 'FALSE'
            snowpipe_enabled = 'TRUE' if config_data.get('snowpipe', False) else 'FALSE'
            stream_enabled = 'TRUE' if config_data.get('stream', False) else 'FALSE'
            task_enabled = 'TRUE' if config_data.get('task', False) else 'FALSE'
            
            session.sql(f"""
                INSERT INTO {schema}.CDC_CONFIGURATIONS (
                    CONFIG_NAME, SOURCE_URL, PRIMARY_KEY, STAGING_SCHEMA, TARGET_SCHEMA,
                    WAREHOUSE, SCHEDULE, HISTORICAL_COPY, SNOWPIPE, STREAM, TASK_ENABLED, FILE_FORMAT,
                    DESCRIPTION, CREATED_AT, LAST_MODIFIED
                ) VALUES (
                    '{config_name}', '{config_data['source_url']}', '{config_data['primary_key']}', 
                    '{config_data['schema']}', '{config_data['gold_schema']}', '{config_data['warehouse']}', 
                    '{config_data['schedule']}', {historical_copy}, {snowpipe_enabled},{stream_enabled}, {task_enabled}, '{config_data['file_format']}',
                    '{config_data['description']}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                )
            """).collect()
            log_action("Config Save", "Success", f"Configuration '{config_name}' saved")
            return True
        except Exception as e:
            log_action("Config Save", "Failed", str(e))
            raise
    
    def load_configs_from_table(session, schema):
        try:
            configs = session.sql(f"SELECT * FROM {schema}.CDC_CONFIGURATIONS").collect()
            # st.session_state.cdc_configs = {
            #     config['CONFIG_NAME']: {
            #         "source_url": config['SOURCE_URL'],
            #         "primary_key": config['PRIMARY_KEY'],
            #         "schema": config['STAGING_SCHEMA'],
            #         "gold_schema": config['TARGET_SCHEMA'],
            #         "warehouse": config['WAREHOUSE'],
            #         "schedule": config['SCHEDULE'],
            #         "historical_copy": config['HISTORICAL_COPY'],
            #         "snowpipe": config.get('SNOWPIPE', False),
            #         "stream": config.get('STREAM'),
            #         "task": config['TASK_ENABLED'],
            #         "file_format": config['FILE_FORMAT'],
            #         "description": config['DESCRIPTION'],
            #         "created_at": config['CREATED_AT'],
            #         "last_modified": config['LAST_MODIFIED']
            #     } for config in configs
            # }

            st.session_state.cdc_configs = {
                config['CONFIG_NAME']: {
                    "source_url": config['SOURCE_URL'],
                    "primary_key": config['PRIMARY_KEY'],
                    "schema": config['STAGING_SCHEMA'],
                    "gold_schema": config['TARGET_SCHEMA'],
                    "warehouse": config['WAREHOUSE'],
                    "schedule": config['SCHEDULE'],
                    "historical_copy": config['HISTORICAL_COPY'],
                    "snowpipe": config['SNOWPIPE'],
                    "stream": config['STREAM'],
                    "task": config['TASK_ENABLED'],
                    "file_format": config['FILE_FORMAT'],
                    "description": config['DESCRIPTION'],
                    "created_at": config['CREATED_AT'],
                    "last_modified": config['LAST_MODIFIED']
                } for config in configs
            }

            log_action("Config Load", "Success", f"Loaded {len(configs)} configurations")
            return True
        except Exception as e:
            log_action("Config Load", "Failed", str(e))
            return False

    def to_bool(v):
        """Convert Excel TRUE/FALSE → Python True/False (capitalized)"""
        # if v is None:
        #     return True
        val = str(v).strip().upper()
        if val == "TRUE":
            return True   
        return False 

    # def normalize_config_keys(config):
    #     """Map Excel UPPERCASE → lowercase keys and enforce boolean conversion safely"""
    
    #     key_mapping = {
    #         "HISTORICAL_COPY": "historical_copy",
    #         "SNOWPIPE": "snowpipe",
    #         "STREAM": "stream",
    #         "TASK_ENABLED": "task"
    #     }
    
    #     new_config = config.copy()
    
    #     for excel_key, python_key in key_mapping.items():
    
    #         value = config.get(excel_key)
    
    #         # If already boolean (from Snowflake)
    #         if isinstance(value, bool):
    #             new_config[python_key] = value
    
    #         # If string from Excel
    #         elif isinstance(value, str):
    #             new_config[python_key] = value.strip().upper() in ("TRUE", "1", "YES", "Y")
    
    #         # If None
    #         elif value is None:
    #             new_config[python_key] = False
    
    #         else:
    #             new_config[python_key] = False
    
    #     return new_config


    
    def run_cdc(session, config_name, config, full_file_format, execution_log):
        """
        Complete CDC Pipeline: Stage → Table → Stream → Gold → Snowpipe → Task
        """
        # config = normalize_config_keys(config) 

        
        # use_stream = config["stream"]  
        use_stream = bool(config.get("stream", False))

        
        try:
            # Parse names
            base_name = config["source_url"].rstrip('/').split('/')[-1]
            stage_name = f"{base_name}_STAGE"
            stream_name = f"{base_name}_STREAM"
            task_name = f"{base_name}_TASK"
            pipe_name = f"{base_name}_PIPE"
            staging_table = f"{base_name}_STAGING" if use_stream else base_name
            target_table = base_name
            
            # Parse schemas
            if '.' in config['schema']:
                staging_db, staging_schema = config['schema'].split('.', 1)
            else:
                staging_db = st.session_state.config_db
                staging_schema = config['schema']
                
            if '.' in config['gold_schema']:
                target_db, target_schema = config['gold_schema'].split('.', 1)
            else:
                target_db = st.session_state.config_db
                target_schema = config['gold_schema']
            
            # Full qualified names
            config['schema'] = f"{staging_db}.{staging_schema}"
            config['gold_schema'] = f"{target_db}.{target_schema}"
            
            # ------------------------------------------------
            # 2. INFRA CREATION (DB/Schema/Stage)
            # ------------------------------------------------
            check_or_create_database(session, staging_db)
            check_or_create_schema(session, staging_db, staging_schema)
            check_or_create_database(session, target_db)
            check_or_create_schema(session, target_db, target_schema)
            execution_log.append(("Schemas", "Success", f"{config['schema']} → {config['gold_schema']}"))


        
            # Stage
            session.sql(f"""
                CREATE STAGE {config['schema']}.{stage_name} 
                URL = '{config['source_url']}' 
                STORAGE_INTEGRATION = {st.session_state.storage_integration}
                FILE_FORMAT = (FORMAT_NAME = '{full_file_format}');
            """).collect()
            execution_log.append(("Stage", "Success", stage_name))
            
            
            # Staging Table (Infer Schema + Normalize)
            session.sql(f"""
                CREATE  TABLE {config['schema']}.{staging_table} 
                USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                    FROM TABLE(
                        INFER_SCHEMA(
                            LOCATION => '@{config["schema"]}.{stage_name}',
                            FILE_FORMAT => '{full_file_format}'
                        )
                    )
                )
                WITH ENABLE_SCHEMA_EVOLUTION = TRUE;
            """).collect()
            
            # Normalize NUMBER columns
            cols = session.sql(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{staging_schema}' AND table_catalog = '{staging_db}'
                AND table_name = '{staging_table}' AND data_type = 'NUMBER'
            """).collect()
            
            for row in cols:
                session.sql(f"""
                    ALTER TABLE {config['schema']}.{staging_table}
                    ALTER COLUMN {row['COLUMN_NAME']} SET DATA TYPE NUMBER(10,2)
                """).collect()
                
            execution_log.append(("Staging Table", "Success", staging_table))

                        # ------------------------------------------------
            # 3. HISTORICAL LOAD
            # ------------------------------------------------
            if config["historical_copy"]:
                session.sql(f"""
                    COPY INTO {config['schema']}.{staging_table} 
                    FROM @{config['schema']}.{stage_name} 
                    FILE_FORMAT = (FORMAT_NAME = '{full_file_format}') 
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                """).collect()
                execution_log.append(("Historical Load", "Success", "✅ Loaded"))
            
            # ------------------------------------------------
            # 4. TARGET TABLE
            # ------------------------------------------------
            if use_stream:
                session.sql(f"""
                    CREATE TABLE IF NOT EXISTS {config['gold_schema']}.{target_table}
                    LIKE {config['schema']}.{staging_table};
                """).collect()
                execution_log.append(("Target Table", "Success", f"✅ {config['gold_schema']}.{target_table}"))
            else:
                execution_log.append(("Target Table", "Skipped", "STREAM=False"))
            
            # ------------------------------------------------
            # 4. STREAM CREATION
            # ------------------------------------------------
            if use_stream:
                session.sql(f"""
                    CREATE  STREAM {config['schema']}.{stream_name}
                    ON TABLE {config['schema']}.{staging_table}
                    APPEND_ONLY = TRUE;
                """).collect()
                execution_log.append(("Stream", "Success", f"✅ {config['schema']}.{stream_name}"))
            else:
                execution_log.append(("Stream", "Skipped", "STREAM=FALSE"))
            

            # ------------------------------------------------
            # 6. SNOWPIPE
            # ------------------------------------------------
            if config["snowpipe"]:
                full_pipe = f'{config["schema"]}.{pipe_name}'
            
                session.sql(f"""
                    CREATE OR REPLACE PIPE {full_pipe}
                    AUTO_INGEST = TRUE
                    AS
                    COPY INTO {config['schema']}.{staging_table}
                    FROM @{config['schema']}.{stage_name}
                    FILE_FORMAT = (FORMAT_NAME = '{full_file_format}')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                """).collect()
            
                # ---------- SAFE DESC PIPE ----------
                df = session.sql(f"DESC PIPE {full_pipe}").to_pandas()
                
                notification_channel = None
                definition = None
                
                if not df.empty:
                
                    # Clean column names (remove quotes + lowercase)
                    df.columns = [c.replace('"', '').strip().lower() for c in df.columns]
                
                    # Identify correct columns dynamically
                    name_col = next((c for c in df.columns if "name" in c), None)
                    value_col = next((c for c in df.columns if "value" in c), None)
                
                    if name_col and value_col:
                
                        for _, row in df.iterrows():
                            key = str(row[name_col]).strip().lower()
                            val = row[value_col]
                
                            if key == "notification_channel":
                                notification_channel = val
                            elif key == "definition":
                                definition = val




            # if config["snowpipe"]:
            #     full_pipe = f'{config["schema"]}.{pipe_name}'
            #     session.sql(f"""
            #         CREATE  PIPE {full_pipe}
            #         AUTO_INGEST = TRUE
            #         AS
            #         COPY INTO {config['schema']}.{staging_table}
            #         FROM @{config['schema']}.{stage_name}
            #         FILE_FORMAT = (FORMAT_NAME = '{full_file_format}')
            #         MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
            #     """).collect()
                
            #     # Get notification channel
            #     df = session.sql(f"DESC PIPE {full_pipe}").to_pandas()
            #     df.columns = [c.lower() for c in df.columns]
            #     props = dict(zip(df['property'], df['property_value']))
            #     notification_channel = props.get("notification_channel", "N/A")
                
            #     st.session_state.snowpipe_info = st.session_state.get("snowpipe_info", {})
            #     st.session_state.snowpipe_info[config_name] = {
            #         "pipe": full_pipe, "notification": notification_channel
            #     }
            #     execution_log.append(("Snowpipe", "Success", f"✅ {full_pipe} | Channel: {notification_channel}"))
            # else:
            #     execution_log.append(("Snowpipe", "Skipped", "snowpipe=FALSE "))
            
            # ------------------------------------------------
            # 7. TASK + MERGE (Only if STREAM + TASK enabled)
            # ------------------------------------------------
            if config["task"]:
                db_name, schema_name = config['schema'].split('.', 1)
                
                # Get columns (exclude metadata)
                columns_df = session.sql(f"""
                    SELECT COLUMN_NAME 
                    FROM {db_name}.INFORMATION_SCHEMA.COLUMNS
                    WHERE LOWER(TABLE_SCHEMA) = LOWER('{schema_name}')
                    AND LOWER(TABLE_NAME) = LOWER('{staging_table}')
                    AND COLUMN_NAME NOT LIKE 'METADATA$%'
                """).collect()
                
                column_names = [col['COLUMN_NAME'] for col in columns_df]
                column_list = ", ".join(column_names)
                update_list = ", ".join([f"TARGET.{col} = STAGED.{col}" for col in column_names])
                
                primary_keys = [k.strip() for k in config["primary_key"].split(",")]
                partition_clause = ", ".join(primary_keys)
                on_clause = " AND ".join([f"TARGET.{k} = STAGED.{k}" for k in primary_keys])
                
                merge_sql = f"""
                MERGE INTO {config['gold_schema']}.{target_table} AS TARGET 
                USING (
                    SELECT {column_list}, METADATA$ACTION
                    FROM {config['schema']}.{stream_name}
                    WHERE METADATA$ACTION IN ('INSERT', 'DELETE')
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY {partition_clause}
                        ORDER BY CASE METADATA$ACTION WHEN 'DELETE' THEN 1 ELSE 2 END DESC
                    ) = 1
                ) AS STAGED 
                ON {on_clause}
                WHEN MATCHED AND STAGED.METADATA$ACTION = 'DELETE' THEN DELETE 
                WHEN MATCHED AND STAGED.METADATA$ACTION = 'INSERT' THEN UPDATE SET {update_list}
                WHEN NOT MATCHED AND STAGED.METADATA$ACTION = 'INSERT' THEN 
                    INSERT ({column_list}) VALUES ({column_list});
                """
                
                session.sql(f"""
                    CREATE  TASK {config['schema']}.{task_name}
                    WAREHOUSE = '{config['warehouse']}'
                    SCHEDULE = '{config['schedule']}'
                    AS {merge_sql}
                """).collect()
                
                session.sql(f"ALTER TASK {config['schema']}.{task_name} RESUME").collect()
                execution_log.append(("Task", "Success", f"✅ {config['schema']}.{task_name} resumed"))
            else:
                execution_log.append(("Task", "Skipped", "task=FALSE or STREAM=FALSE"))
            
            log_action("CDC Pipeline", "Success", f"✅ Complete: {config_name}", config_name)
            
        except Exception as e:
            error_msg = f"❌ {config_name}: {str(e)}"
            st.error(error_msg)
            log_action("CDC Pipeline", "Failed", error_msg, config_name)
            execution_log.append(("Pipeline", "Failed", error_msg))


    # ------------------------------------------------
    # END OF MAIN CDC EXECUTION LOGIC
    # ------------------------------------------------
    
    def main():
    
        CONFIG_DB = st.session_state.get("config_db")
        CONFIG_SCHEMA = st.session_state.get("config_schema")
    
        if not CONFIG_DB or not CONFIG_SCHEMA:
            st.error("Setup not completed — config DB/schema missing")
            st.stop()
    
        if st.button("⬅️ Back to Storage Integration"):
            st.session_state.setup_complete = False
            st.rerun()

    
        
        st.markdown("""
        <style>
            .stApp { background-color: #f8f9fa; }
            .header { color: #2c3e50; font-weight: 600; }
            .info-box { background-color: #e8f4f8; padding: 20px; border-radius: 10px; margin-bottom: 20px; border-left: 4px solid #3498db; }
            .success-box { background-color: #e8f8f0; padding: 20px; border-radius: 10px; margin-bottom: 20px; border-left: 4px solid #28a745; }
            .error-box { background-color: #f8e8e8; padding: 20px; border-radius: 10px; margin-bottom: 20px; border-left: 4px solid #dc3545; }
            .warning-box { background-color: #fff3cd; padding: 20px; border-radius: 10px; margin-bottom: 20px; border-left: 4px solid #ffc107; }
            .section { margin-bottom: 30px; border-bottom: 1px solid #eee; padding-bottom: 20px; }
            .stButton>button { background-color: #4CAF50; color: white; font-weight: 500; border-radius: 8px; padding: 10px 24px; border: none; transition: all 0.3s; }
            .stButton>button:hover { background-color: #45a049; box-shadow: 0 4px 8px rgba(0,0,0,0.2); }
            .secondary-button>button { background-color: #6c757d; color: white; }
            .secondary-button>button:hover { background-color: #5a6268; }
            .tab-content { padding: 20px; background-color: white; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .object-card { background-color: white; border-radius: 8px; padding: 20px; margin-bottom: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); transition: all 0.3s; }
            .object-card:hover { box-shadow: 0 4px 8px rgba(0,0,0,0.15); }
            .object-title { font-weight: bold; font-size: 1.1em; margin-bottom: 10px; color: #2c3e50; }
            .status-active { color: #28a745; font-weight: bold; }
            .status-inactive { color: #dc3545; font-weight: bold; }
            .status-warning { color: #ffc107; font-weight: bold; }
            .config-badge { background-color: #e3f2fd; padding: 5px 12px; border-radius: 15px; display: inline-block; margin: 5px; font-size: 0.9em; }
            .metric-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; text-align: center; }
            .metric-value { font-size: 2em; font-weight: bold; }
            .metric-label { font-size: 0.9em; opacity: 0.9; }
        </style>
        """, unsafe_allow_html=True)
        
        initialize_session_state()
        session = get_active_session()
        
        st.markdown("<h1 class='header'>🔄 Snowflake CDC Management Console</h1>", unsafe_allow_html=True)
        st.markdown("""
        <div class='info-box'>
            <strong>📊 Change Data Capture (CDC) Configuration and Monitoring</strong><br>
            Configure, execute, and monitor CDC processes for multiple data sources in Snowflake.
            Set up staging tables, streams, Snowpipes, and automated tasks to keep your data synchronized.
        </div>
        """, unsafe_allow_html=True)

        # --------------------------------
        #Config table creation
        #--------------------------------
        
        full_schema = f"{CONFIG_DB}.{CONFIG_SCHEMA}"
    
        check_or_create_database(session, CONFIG_DB)
        check_or_create_schema(session, CONFIG_DB, CONFIG_SCHEMA)
        create_config_table(session, full_schema)
        load_configs_from_table(session, full_schema)

 
    

        # -------------------------------
        # Dashboard Metrics
        # -------------------------------
        
        total_configs = len(st.session_state.get("cdc_configs", {}))
        imported_configs = len(st.session_state.get("imported_configs", []))
        
        active_tasks = sum(
            1 for cfg in st.session_state.get("cdc_configs", {}).values()
            if cfg.get("task", False)
        )
        
        col1, col2, col3 = st.columns(3)
        
        col1.metric(
            label="📦 Total Configurations",
            value=total_configs,
            help="Total CDC configurations created in this session"
        )
        
        col2.metric(
            label="📥 Imported Configurations",
            value=imported_configs,
            help="Configs loaded from Excel import"
        )
        
        col3.metric(
            label="⚡ Active Tasks Enabled",
            value=active_tasks,
            help="Configurations where Snowflake TASK is enabled"
        )

 
        # Main Interface
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 ,tab8 = st.tabs(["⚙️ Configuration" , "🧾 File Format", "📥 Import", "▶️ Execution", "📡 Snowpipe","🎯 Single Table", "📊 Monitoring", "📝 Logs"])

        with tab1:
            st.markdown("<h2 class='header'>⚙️ CDC Configuration Management</h2>", unsafe_allow_html=True)
            
            # Database/Schema Selection
            col_db, col_schema = st.columns(2)
            with col_db:
                databases = get_databases(session)
                try:
                    db_index = databases.index(st.session_state.config_db)
                except ValueError:
                    db_index = 0
                    st.session_state.config_db = databases[0] if databases else ""
                
                selected_db = st.selectbox("💾 Database", databases, index=db_index)
            
            with col_schema:
                schemas = get_schemas(session, selected_db)
                try:
                    schema_index = schemas.index(st.session_state.config_schema)
                except ValueError:
                    schema_index = 0
                    st.session_state.config_schema = schemas[0] if schemas else ""
                
                selected_schema = st.selectbox("📁 Config Schema", schemas, index=schema_index) 
    
            # def get_schemas(session, database):
            #     try:
            #         return [s['name'] for s in session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()]
            #     except Exception as e:
            #         log_action("Get Schemas", "Failed", str(e))
            #         return []
    
                    # ------------------------------------------------
            # STORAGE INTEGRATION FETCH (RESTORED)
            # ------------------------------------------------
            
            
            def get_storage_integrations(session):
                try:
                    df = session.sql(f"""
                        SELECT NAME
                        FROM {st.session_state.config_db}.{st.session_state.config_schema}.INTERATION_INFO
                        ORDER BY CREATED_ON DESC
                    """).to_pandas()
            
                    if df.empty:
                        return []
            
                    return df["NAME"].tolist()
            
                except Exception as e:
                    log_action("Get Integrations", "Failed", str(e))
                    return []
    
    
            
            # Storage Integration
            integrations = get_storage_integrations(session)
            selected_integration = st.selectbox("📦 Storage Integration", integrations, 
                                              index=integrations.index(st.session_state.storage_integration))
            
        
            st.session_state.config_db = selected_db
            st.session_state.config_schema = selected_schema
            st.session_state.storage_integration = selected_integration
        
            # Configuration Management
            st.markdown("---")
            col1, col2 = st.columns([3, 1])
            with col1:
                config_name = st.text_input("📝 Configuration Name", help="Unique name for this CDC configuration", placeholder="Enter configuration name...")
            with col2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("➕ Create New Config", use_container_width=True):
                    if config_name and config_name not in st.session_state.cdc_configs:
                        st.session_state.cdc_configs[config_name] = {
                            "source_url": "",
                            "primary_key": "",
                            "schema": "SILVER",
                            "gold_schema": "GOLD",
                            "warehouse": "COMPUTE_WH",
                            "schedule": "USING CRON 0 * * * * UTC",
                            "historical_copy": True,
                            "snowpipe": False,
                            "stream":False,
                            "task": True,
                            "file_format": "CDC_FORMAT",
                            "description": "",
                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "last_modified": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        st.session_state.active_config = config_name
                        st.success(f"✅ Configuration '{config_name}' created")
        
            if st.session_state.cdc_configs:
                selected_config = st.selectbox("📋 Select Configuration", list(st.session_state.cdc_configs.keys()))
                config = st.session_state.cdc_configs[selected_config]
                
                with st.form(key="config_form"):
                    col1, col2 = st.columns(2)
                    with col1:
                        config["source_url"] = st.text_input("🔗 Source S3 Path", config["source_url"], placeholder="s3://bucket/path/")
                        config["primary_key"] = st.text_input("🔑 Primary Key", config["primary_key"], placeholder="ID or ID,TIMESTAMP")
                        
                        # Staging Schema selection
                        staging_schemas = get_schemas(session, selected_db)
                        try:
                            staging_index = staging_schemas.index(config["schema"])
                        except ValueError:
                            staging_index = 0
                        new_staging_schema = st.selectbox(
                            "📂 Staging Schema", 
                            staging_schemas, 
                            index=staging_index,
                            key="staging_schema_select"
                        )
                        st.session_state.selected_staging_schema = new_staging_schema
                    
                    with col2:
                        # Target Schema selection
                        target_schemas = get_schemas(session, selected_db)
                        try:
                            target_index = target_schemas.index(config["gold_schema"])
                        except ValueError:
                            target_index = 0
                        new_target_schema = st.selectbox(
                            "🎯 Target Schema", 
                            target_schemas, 
                            index=target_index,
                            key="target_schema_select"
                        )
                        st.session_state.selected_target_schema = new_target_schema
                        
                        config["warehouse"] = st.text_input("🏭 Warehouse", config["warehouse"], placeholder="COMPUTE_WH")
                        config["schedule"] = st.text_input("⏰ Task Schedule", config["schedule"], placeholder="USING CRON 0 * * * * UTC")
                    
                    config["description"] = st.text_area("📄 Description", config["description"], placeholder="Describe this CDC configuration...")
                    
                    col_check1, col_check2, col_check3,col_check4  = st.columns(4)
                    with col_check1:
                        config["historical_copy"] = st.checkbox("📦 Initial Historical Load", config["historical_copy"])
                    with col_check2:
                        config["snowpipe"] = st.checkbox("🔄 Enable Snowpipe", config.get("snowpipe", False))
                    with col_check3:
                        config["stream"] = st.checkbox("🔄 Enable stream", config.get("stream", False))
                    with col_check4:
                        config["task"] = st.checkbox("⚡ Enable Scheduled Task", config["task"])
        
                    if st.form_submit_button("💾 Save Configuration", use_container_width=True):
                        try:
                            selected_db = st.session_state.config_db
                            config["schema"] = f"{selected_db}.{st.session_state.selected_staging_schema}"
                            config["gold_schema"] = f"{selected_db}.{st.session_state.selected_target_schema}"
                            
                            save_config_to_table(session, f"{selected_db}.{selected_schema}", selected_config, config)
                            st.success("✅ Configuration saved successfully!")
                        except Exception as e:
                            st.error(f"❌ Save failed: {str(e)}")
        
                # Schema creation buttons OUTSIDE the form
                col_create_schemas = st.columns(2)
                with col_create_schemas[0]:
                    if st.button("➕ Create Staging Schema", use_container_width=True):
                        schema_name = st.session_state.selected_staging_schema
                        if schema_name not in get_schemas(session, selected_db):
                            check_or_create_schema(session, selected_db, schema_name)
                            st.success(f"✅ Staging schema '{schema_name}' created!")
                            st.rerun()
                
                with col_create_schemas[1]:
                    if st.button("➕ Create Target Schema", use_container_width=True):
                        schema_name = st.session_state.selected_target_schema
                        if schema_name not in get_schemas(session, selected_db):
                            check_or_create_schema(session, selected_db, schema_name)
                            st.success(f"✅ Target schema '{schema_name}' created!")
                            st.rerun()

        with tab2:
            st.markdown("---")
            st.subheader("📂 File Format Builder")
            
            colf1, colf2 = st.columns(2)
            
            with colf1:
                ff_name = st.text_input(
                    "File Format Name",
                    value="CDC_FORMAT",
                    key="ff_name_input"
                )
            
            with colf2:
                ff_type = st.selectbox(
                    "📄 File Type",
                    ["CSV", "JSON", "PARQUET"],
                    key="ff_type_select"
                )
            
            # ------------------------------------------------
            # CSV OPTIONS (Your Required Defaults)
            # ------------------------------------------------
            
            if ff_type == "CSV":
            
                st.markdown("### 📄 CSV Format Settings")
            
                # delimiter = st.text_input("Delimiter", ",", key="csv_delim")
                parse_header = st.checkbox("PARSE_HEADER", True, key="csv_parse_header")
                trim_space = st.checkbox("TRIM_SPACE", True, key="csv_trim")
                replace_invalid = st.checkbox("REPLACE_INVALID_CHARACTERS", True, key="csv_replace")
                escape_unenclosed_field = st.checkbox("ESCAPE_UNENCLOSED_FIELD", True, key="csv_escape_unenclosed_field")
                field_optionally_enclosed_by = st.checkbox("FIELD_OPTIONALLY_ENCLOSED_BY", True, key="csv_field_optionally_enclosed_by")
                error_on_column_count_mismatch = st.checkbox("ERROR_ON_COLUMN_COUNT_MISMATCH", True, key="csv_error_on_column_count_mismatch")
                null_if = st.checkbox("NULL_IF", True, key="csv_null_if")
                replace_invalid_characters = st.checkbox("REPLACE_INVALID_CHARACTERS", True, key="csv_replace_invalid_characters")
        
        
        
            
            # ------------------------------------------------
            # JSON OPTIONS
            # ------------------------------------------------
            
            if ff_type == "JSON":
            
                st.markdown("### 🧾 JSON Settings")
            
                strip_array = st.checkbox("STRIP_OUTER_ARRAY", True, key="json_strip")
                ignore_utf = st.checkbox("IGNORE_UTF8_ERRORS", True, key="json_utf")
            
            # ------------------------------------------------
            # PARQUET OPTIONS
            # ------------------------------------------------
            
            if ff_type == "PARQUET":
            
                st.markdown("### 🧱 Parquet Settings")
        
                logical = st.checkbox("USE_LOGICAL_TYPE", True, key="pq_logical")
                trim = st.checkbox("TRIM_SPACE", True, key="pq_trim")
                type = st.checkbox("TYPE", True, key="pq_type")
                compression = st.checkbox("COMPRESSION", True, key="pq_compression")
                binary_as_text = st.checkbox("BINARY_AS_TEXT", True, key="pq_binary_as_text")
                use_vectorized_scanner = st.checkbox("USE_VECTORIZED_SCANNER", True, key="pq_use_vectorized_scanner")
                replace_invalid_characters = st.checkbox("REPLACE_INVALID_CHARACTERS", True, key="pq_replace_invalid_characters")
                null_if = st.checkbox("NULL_IF", True, key="pq_null_if")
        
            
            # ------------------------------------------------
            # CREATE FILE FORMAT BUTTON
            # ------------------------------------------------
            
            if st.button("🚀 Create File Format", key="create_ff_btn", type="primary"):
            
                full_schema = f"{CONFIG_DB}.{CONFIG_SCHEMA}"
            
                try:
            
                    if ff_type == "CSV":
            
                        sql = f"""
            CREATE  FILE FORMAT {full_schema}.{ff_name}
            TYPE = CSV
            PARSE_HEADER = TRUE
            ESCAPE_UNENCLOSED_FIELD = 'NONE'
            TRIM_SPACE = TRUE
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('NULL','')
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            REPLACE_INVALID_CHARACTERS = TRUE
            """
            
                    elif ff_type == "JSON":
            
                        sql = f"""
            CREATE FILE FORMAT {full_schema}.{ff_name}
            TYPE = JSON
            STRIP_OUTER_ARRAY = {str(strip_array).upper()}
            IGNORE_UTF8_ERRORS = {str(ignore_utf).upper()}
            """
            
                    else:  # PARQUET
            
                        sql = f"""
            CREATE  FILE FORMAT {full_schema}.{ff_name}
                            TYPE = 'PARQUET'
                            COMPRESSION = 'AUTO'
                            -- SNAPPY_COMPRESSION = TRUE
                            BINARY_AS_TEXT = FALSE
                            USE_LOGICAL_TYPE =  {str(logical).upper()}
                            TRIM_SPACE = {str(trim).upper()}
                            USE_VECTORIZED_SCANNER = TRUE
                            REPLACE_INVALID_CHARACTERS = TRUE
                            NULL_IF = ('NULL', 'null', '')"""
            
                    st.code(sql, language="sql")  # preview
                    session.sql(sql).collect()
            
                    st.success(f"✅ File Format `{ff_name}` created")
            
                except Exception as e:
                    st.error(str(e))
        
        with tab3:
            st.markdown("<h2 class='header'>📥 Import Configurations</h2>", unsafe_allow_html=True)
            
            st.markdown("""
            <div class='info-box'>
                <strong>ℹ️ Import Instructions</strong><br>
                Upload an Excel file with CDC configurations. The imported configurations will replace existing ones in the configuration table.
                <br><br>
                <strong>✨ Snowpipe Support:</strong> Set the SNOWPIPE column to TRUE to enable automatic data ingestion with Snowpipe.
            </div>
            """, unsafe_allow_html=True)
            
            uploaded_file = st.file_uploader("📂 Upload Excel File", type=['xlsx'], help="Upload an Excel file containing CDC configurations")
            
            if uploaded_file:
                try:
                    df = pd.read_excel(uploaded_file)
                    
                    st.markdown("### 📋 Preview Imported Data")
                    st.dataframe(df, use_container_width=True)
                    
                    st.info(f"📊 Found {len(df)} configuration(s) in the uploaded file")
                    
                    if st.button("🚀 File uploaded successfully — click Execute to start integration", use_container_width=True):
                        with st.spinner("Importing configurations..."):
                            # Clear existing configs and imported list
                            st.session_state.cdc_configs = {}
                            st.session_state.imported_configs = []
                            
                            # Truncate configuration table before importing
                            try:
                                truncate_config_table(session, f"{st.session_state.config_db}.{st.session_state.config_schema}")
                                st.success("✅ Configuration table cleared")
                            except Exception as e:
                                st.error(f"❌ Failed to clear configuration table: {str(e)}")
                            
                            # Import new configurations
                            for _, row in df.iterrows():
                                config_name = row['CONFIG_NAME']
                                staging_schema = f"{st.session_state.config_db}.{row['STAGING_SCHEMA']}"
                                target_schema = f"{st.session_state.config_db}.{row['TARGET_SCHEMA']}"
                                
                                st.session_state.cdc_configs[config_name] = {
                                    "source_url": row['SOURCE_URL'],
                                    "primary_key": row['PRIMARY_KEY'],
                                    "schema": staging_schema,
                                    "gold_schema": target_schema,
                                    "warehouse": row['WAREHOUSE'],
                                    "schedule": row['SCHEDULE'],
                                    "historical_copy": bool(row['HISTORICAL_COPY']),
                                    "snowpipe": bool(row.get('SNOWPIPE', False)),
                                    "stream": bool(row.get('STREAM', False)),
                                    "task": bool(row['TASK_ENABLED']),
                                    "file_format": row['FILE_FORMAT'],
                                    "description": row['DESCRIPTION'],
                                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "last_modified": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                
                                # Track imported configs
                                st.session_state.imported_configs.append(config_name)
                                
                                save_config_to_table(session, f"{st.session_state.config_db}.{st.session_state.config_schema}",
                                                     config_name, st.session_state.cdc_configs[config_name])
                            
                            st.success(f"✅ Imported {len(df)} configuration(s) successfully!")
                            
                            # Show imported configurations
                            st.markdown("### 📦 Imported Configurations")
                            for config_name in st.session_state.imported_configs:
                                config = st.session_state.cdc_configs[config_name]
                                snowpipe_indicator = " 🔄 [Snowpipe]" if config.get('snowpipe', False) else ""
                                st.markdown(f"<div class='config-badge'>✓ {config_name}{snowpipe_indicator}</div>", unsafe_allow_html=True)
                            
                            st.rerun()
                except Exception as e:
                    st.error(f"❌ Import error: {str(e)}")
            
            st.markdown("---")
            st.markdown("### 📥 Download Template")
            st.markdown("Download this template to understand the required format for importing configurations.")
            
            # Template Download
            sample_data = {
                "CONFIG_NAME": ["EXAMPLE_CONFIG"],
                "SOURCE_URL": ["s3://your-bucket/path/"],
                "PRIMARY_KEY": ["ID"],
                "STAGING_SCHEMA": ["SILVER"],
                "TARGET_SCHEMA": ["GOLD"],
                "WAREHOUSE": ["COMPUTE_WH"],
                "SCHEDULE": ["USING CRON 0 * * * * UTC"],
                "HISTORICAL_COPY": [True],
                "SNOWPIPE": [False],
                "STREAM": [False],
                "TASK_ENABLED": [False],
                "FILE_FORMAT": ["CDC_FORMAT"],
                "DESCRIPTION": ["Example configuration"]
            }
            sample_df = pd.DataFrame(sample_data)
            bool_cols = ['HISTORICAL_COPY', 'SNOWPIPE', 'STREAM', 'TASK_ENABLED']
            sample_df[bool_cols] = sample_df[bool_cols].astype(str) 
            
            output = io.BytesIO()
            try:
                with pd.ExcelWriter(output, engine='xlsxwriter', engine_kwargs={'options': {'in_memory': True}}) as writer:
                    sample_df.to_excel(writer, index=False)
            except Exception as e:
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    sample_df.to_excel(writer, index=False)
            
            output.seek(0)
            st.download_button(
                "📥 Download Template",
                data=output,
                file_name="Integration_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    
        with tab4:
            st.markdown("<h2 class='header'>▶️Execution</h2>", unsafe_allow_html=True)
        
            if not st.session_state.cdc_configs:
                st.info("ℹ️ No configurations available. Please create a configuration first or import from Excel.")
            else:
                # Show execution mode selection
                st.markdown("### 🎯 Execution Mode")
                
                col_mode1, col_mode2 = st.columns(2)
                with col_mode1:
                    st.markdown("""
                    <div class='info-box'>
                        <strong>📦 Execute Imported Configurations Only</strong><br>
                        Runs CDC process only for configurations imported from Excel file
                    </div>
                    """, unsafe_allow_html=True)
                with col_mode2:
                    st.markdown("""
                    <div class='info-box'>
                        <strong>🔄 Execute All Configurations</strong><br>
                        Runs CDC process for all available configurations
                    </div>
                    """, unsafe_allow_html=True)

                file_format = config.get("file_format")

                if "." not in file_format:
                    file_format = f"{st.session_state.config_db}.{st.session_state.config_schema}.{file_format}"

                
                # full_file_format = f"{st.session_state.config_db}.{st.session_state.config_schema}.CDC_FORMAT"
                
                # Display configurations
                st.markdown("### 📋 Available Configurations")
                for config_name, config in st.session_state.cdc_configs.items():
                    is_imported = config_name in st.session_state.imported_configs
                    badge = "🆕 IMPORTED" if is_imported else "📌 EXISTING"
                    
                    with st.expander(f"{badge} {config_name}", expanded=False):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown(f"""
                            <div class='object-card'>
                                <div class='object-title'>🗂️ Source Details</div>
                                <p><strong>File Format:</strong> {config.get('file_format')}</p>
                                <p><strong>Primary Key:</strong> {config['primary_key']}</p>
                                <p><strong>Source Path:</strong> {config['source_url']}</p>
                            </div>
                            """, unsafe_allow_html=True)
            
                        with col2:
                            st.markdown(f"""
                            <div class='object-card'>
                                <div class='object-title'>❄️ Snowflake Details</div>
                                <p><strong>Staging Schema:</strong> {config['schema']}</p>
                                <p><strong>Target Schema:</strong> {config['gold_schema']}</p>
                                <p><strong>Warehouse:</strong> {config['warehouse']}</p>
                            </div>
                            """, unsafe_allow_html=True)
            
                        st.markdown(f"""
                        <div class='object-card'>
                            <div class='object-title'>⚙️ Execution Options</div>
                            <p>
                                {'✅ Historical Load' if config['historical_copy'] else '❌ Historical Load'}<br>
                                {'✅ Snowpipe Enabled' if config.get('snowpipe', False) else '❌ Snowpipe Disabled'}<br>
                                {'✅ stream Enabled' if config.get('stream', False) else '❌ stream Disabled'}<br>
                                {'✅ Task Enabled' if config['task'] else '❌ Task Disabled'}<br>
                                📅 Schedule: {config['schedule']}
                            </p>
                        </div>
                        """, unsafe_allow_html=True)
                
                # Execution buttons
                st.markdown("---")
                st.markdown("### 🚀 Execute CDC Process")
                
                col_exec1, col_exec2 = st.columns(2)
                
                with col_exec1:
                    if st.button("🆕 Execute Imported Configurations Only", use_container_width=True, type="primary"):
                        if not st.session_state.imported_configs:
                            st.warning("⚠️ No imported configurations found. Please import configurations first.")
                        else:
                            with st.spinner(f"Executing {len(st.session_state.imported_configs)} imported configuration(s)..."):
                                execution_log = []
                                
                                # Execute only imported configs
                                for config_name in st.session_state.imported_configs:
                                    if config_name in st.session_state.cdc_configs:
                                        config = st.session_state.cdc_configs[config_name]
                                        st.info(f"🔄 Processing: {config_name}")
                                        run_cdc(session, config_name, config, file_format, execution_log)
                                
                                # Truncate configuration table after execution
                                try:
                                    truncate_config_table(session, f"{st.session_state.config_db}.{st.session_state.config_schema}")
                                    execution_log.append(("Config Table Cleanup", "Success", "Configuration table truncated after execution"))
                                    st.success("✅ Configuration table cleaned up")
                                except Exception as e:
                                    execution_log.append(("Config Table Cleanup", "Failed", str(e)))
                                    st.error(f"❌ Failed to truncate configuration table: {str(e)}")
                                
                                # Clear imported configs list after execution
                                st.session_state.imported_configs = []
                                
                                st.markdown("### 📊 Execution Summary")
                                success_count = sum(1 for entry in execution_log if entry[1] == "Success")
                                failed_count = sum(1 for entry in execution_log if entry[1] == "Failed")
                                
                                col_sum1, col_sum2, col_sum3 = st.columns(3)
                                with col_sum1:
                                    st.metric("Total Steps", len(execution_log))
                                with col_sum2:
                                    st.metric("✅ Successful", success_count)
                                with col_sum3:
                                    st.metric("❌ Failed", failed_count)
                                
                                st.markdown("### 📝 Detailed Execution Log")
                                for entry in execution_log:
                                    status_color = "#28a745" if entry[1] == "Success" else ("#ffc107" if entry[1] == "Warning" else "#dc3545")
                                    status_icon = "✅" if entry[1] == "Success" else ("⚠️" if entry[1] == "Warning" else "❌")
                                    bg_color = "#e8f8f0" if entry[1] == "Success" else ("#fff3cd" if entry[1] == "Warning" else "#f8e8e8")
                                    st.markdown(f"""
                                    <div style="padding: 15px; margin: 10px 0; border-left: 4px solid {status_color}; background-color: {bg_color}; border-radius: 5px;">
                                        <strong>{status_icon} {entry[0]}</strong> ({entry[1]})<br>
                                        {entry[2]}
                                    </div>
                                    """, unsafe_allow_html=True)
                
                with col_exec2:
                    if st.button("🔄 Execute All Configurations", use_container_width=True):
                        with st.spinner(f"Executing all {len(st.session_state.cdc_configs)} configuration(s)..."):
                            execution_log = []
                            
                            for config_name, config in st.session_state.cdc_configs.items():
                                st.info(f"🔄 Processing: {config_name}")
                                run_cdc(session, config_name, config, full_file_format, execution_log)

                            
                            st.markdown("### 📊 Execution Summary")
                            success_count = sum(1 for entry in execution_log if entry[1] == "Success")
                            failed_count = sum(1 for entry in execution_log if entry[1] == "Failed")
                            
                            col_sum1, col_sum2, col_sum3 = st.columns(3)
                            with col_sum1:
                                st.metric("Total Steps", len(execution_log))
                            with col_sum2:
                                st.metric("✅ Successful", success_count)
                            with col_sum3:
                                st.metric("❌ Failed", failed_count)
                            
                            st.markdown("### 📝 Detailed Execution Log")
                            for entry in execution_log:
                                status_color = "#28a745" if entry[1] == "Success" else ("#ffc107" if entry[1] == "Warning" else "#dc3545")
                                status_icon = "✅" if entry[1] == "Success" else ("⚠️" if entry[1] == "Warning" else "❌")
                                bg_color = "#e8f8f0" if entry[1] == "Success" else ("#fff3cd" if entry[1] == "Warning" else "#f8e8e8")
                                st.markdown(f"""
                                <div style="padding: 15px; margin: 10px 0; border-left: 4px solid {status_color}; background-color: {bg_color}; border-radius: 5px;">
                                    <strong>{status_icon} {entry[0]}</strong> ({entry[1]})<br>
                                    {entry[2]}
                                </div>
                                """, unsafe_allow_html=True)


        # with tab5:

        #     st.markdown("<h2 class='header'>📡 Snowpipe</h2>", unsafe_allow_html=True)
        
        #     # ---------------------------------------------------
        #     # 1️⃣ Get All Pipes From Current DB/Schema
        #     # ---------------------------------------------------
        
        #     current_db = st.session_state.get("config_db")
        #     current_schema = st.session_state.get("config_schema")
        
        #     if not current_db or not current_schema:
        #         st.warning("Config DB/Schema not found in session.")
        #         st.stop()
        
        #     pipes_df = session.sql(f"""
        #         SHOW PIPES IN SCHEMA {current_db}.{current_schema}
        #     """).to_pandas()
        
        #     if pipes_df.empty:
        #         st.info("No Snowpipes found in this schema.")
        #         st.stop()
        
        #     # Clean columns
        #     pipes_df.columns = [c.replace('"','').lower() for c in pipes_df.columns]
        
        #     pipe_list = pipes_df["name"].tolist()
        
        #     # ---------------------------------------------------
        #     # 2️⃣ Select Pipe
        #     # ---------------------------------------------------
        
        #     selected_pipe = st.selectbox(
        #         "Select Snowpipe",
        #         pipe_list,
        #         key="snowpipe_selector"
        #     )
        
        #     if not selected_pipe:
        #         st.stop()
        
        #     full_pipe = f"{current_db}.{current_schema}.{selected_pipe}"
        
        #     st.markdown(f"### 🔎 Pipe: `{full_pipe}`")
        
        #     # ---------------------------------------------------
        #     # 3️⃣ DESC PIPE
        #     # ---------------------------------------------------
        
        #     desc_df = session.sql(f"DESC PIPE {full_pipe}").to_pandas()
        
        #     if desc_df.empty:
        #         st.warning("DESC PIPE returned no results.")
        #         st.stop()
        
        #     # Clean column names
        #     desc_df.columns = [c.replace('"','').strip().lower() for c in desc_df.columns]
        
        #     # Identify columns dynamically
        #     name_col = next((c for c in desc_df.columns if "name" in c), None)
        #     value_col = next((c for c in desc_df.columns if "value" in c), None)
        
        #     notification_channel = None
        #     definition = None
        
        #     if name_col and value_col:
        
        #         for _, row in desc_df.iterrows():
        #             key = str(row[name_col]).strip().lower()
        #             val = row[value_col]
        
        #             if key == "notification_channel":
        #                 notification_channel = val
        
        #             if key == "definition":
        #                 definition = val
        
        #     # ---------------------------------------------------
        #     # 4️⃣ Display Results
        #     # ---------------------------------------------------
        
        #     st.markdown("### 📌 Pipe Details")
        
        #     st.success(f"Pipe Name: {full_pipe}")
        
        #     if notification_channel:
        #         st.markdown("**📡 Notification Channel (SQS ARN)**")
        #         st.code(notification_channel)
        #     else:
        #         st.warning("Notification channel not found.")
        
        #     if definition:
        #         st.markdown("### 📄 COPY Definition")
        #         st.code(definition, language="sql")
        #     else:
        #         st.warning("COPY definition not found.")
        
        #     # Optional Debug Section
        #     with st.expander("🔍 Show Raw DESC PIPE Output"):
        #         st.dataframe(desc_df)

        with tab5:
            st.markdown("<h2 class='header'>📡 Snowpipe</h2>", unsafe_allow_html=True)
            
            current_db = st.session_state.get("config_db")
            current_schema = st.session_state.get("config_schema")
            
            if not current_db or not current_schema:
                st.warning("Config DB/Schema not found in session.")
            else:
                pipes_df = session.sql(f"""
                    SHOW PIPES IN SCHEMA {current_db}.{current_schema}
                """).to_pandas()
                
                if pipes_df.empty:
                    st.info("No Snowpipes found in this schema.")
                else:
                    # Clean columns
                    pipes_df.columns = [c.replace('"','').lower() for c in pipes_df.columns]
                    pipe_list = pipes_df["name"].tolist()
                    
                    selected_pipe = st.selectbox(
                        "Select Snowpipe",
                        pipe_list,
                        key="snowpipe_selector"
                    )
                    
                    if selected_pipe:
                        full_pipe = f"{current_db}.{current_schema}.{selected_pipe}"
                        st.markdown(f"### 🔎 Pipe: `{full_pipe}`")
                        
                        desc_df = session.sql(f"DESC PIPE {full_pipe}").to_pandas()
                        
                        if desc_df.empty:
                            st.warning("DESC PIPE returned no results.")
                        else:
                            name_col = next((c for c in desc_df.columns if "name" in c.lower()), None)
                            value_col = next((c for c in desc_df.columns if "value" in c.lower()), None)
                        
                            # notification_channel = None
                            # definition = None
                        
                            # ✅ ONLY extract values here
                            if name_col and value_col:
                                for _, row in desc_df.iterrows():
                                    key = str(row[name_col]).strip().lower()
                                    val = row[value_col]
                        
                                    # if key == "notification_channel":
                                    #     notification_channel = val
                        
                                    # elif key == "definition":
                                    #     definition = val
                        
                            # ✅ SHOW UI AFTER LOOP
                            st.markdown("### 📌 Pipe Details")
                            st.success(f"Pipe Name: {full_pipe}")
                            # if notification_channel:
                            #     st.markdown("**📡 Notification Channel (SQS ARN)**")
                            #     st.code(notification_channel)
                            # else:
                            #     st.warning("Notification channel not found.")
                        
                            # if definition:
                            #     st.markdown("### 📄 COPY Definition")
                            #     st.code(definition, language="sql")
                            # else:
                            #     st.warning("COPY definition not found.")
                        
                            with st.expander("🔍 Show Raw DESC PIPE Output"):
                                st.dataframe(desc_df)
                            

                

        with tab6:
            
            st.markdown("<h2 class='header'>🎯 Single Table Execution</h2>", unsafe_allow_html=True)
            
            st.markdown("""
            <div class='info-box'>
                <strong>ℹ️ Quick Single Table CDC</strong><br>
                Manually configure and execute CDC for a single table without saving to configuration.
                Perfect for testing or one-time operations.
            </div>
            """, unsafe_allow_html=True)
            
            with st.form(key="single_table_form"):
                st.markdown("### 📝 Table Configuration")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    single_config_name = st.text_input(
                        "📌 Configuration Name",
                        placeholder="e.g., ORDERS_TABLE",
                        help="Unique identifier for this execution"
                    )
                    
                    single_source_url = st.text_input(
                        "🔗 Source S3 URL",
                        placeholder="s3://bucket-name/path/to/files/",
                        help="S3 path containing your source files"
                    )
                    
                    single_primary_key = st.text_input(
                        "🔑 Primary Key(s)",
                        placeholder="ORDER_ID or ORDER_ID,LINE_ITEM",
                        help="Comma-separated list of primary key columns"
                    )
                    
                    # Database selection
                    databases = get_databases(session)
                    single_db = st.selectbox(
                        "💾 Database",
                        databases,
                        index=databases.index(st.session_state.config_db) if st.session_state.config_db in databases else 0
                    )
                    
                    # Staging schema selection
                    staging_schemas = get_schemas(session, single_db)
                    single_staging_schema = st.selectbox(
                        "📂 Staging Schema",
                        staging_schemas,
                        help="Schema where staging table and stream will be created"
                    )

                
                with col2:
                    # Target schema selection
                    target_schemas = get_schemas(session, single_db)
                    single_target_schema = st.selectbox(
                        "🎯 Target Schema",
                        target_schemas,
                        help="Schema where final table will be created"
                    )
                    
                    single_warehouse = st.text_input(
                        "🏭 Warehouse",
                        value="COMPUTE_WH",
                        placeholder="COMPUTE_WH",
                        help="Warehouse to use for task execution"
                    )
                    
                    single_schedule = st.text_input(
                        "⏰ Task Schedule",
                        value="USING CRON 0 * * * * UTC",
                        placeholder="USING CRON 0 * * * * UTC",
                        help="Cron schedule for task execution"
                    )
                    # File Format Selection
                    file_formats = get_file_formats(
                        session,
                        single_db,
                        single_staging_schema
                    )
                
                    format_options = file_formats
                
                    single_file_format = st.selectbox(
                        "📄 File Format",
                        format_options,
                        help="Choose existing or auto-create format"
                    )
                    
                    # single_file_format = st.text_input(
                    #     "📄 File Format",
                    #     value="CDC_FORMAT",
                    #     placeholder="CDC_FORMAT",
                    #     help="File format name to use"
                    # )
                
                st.markdown("### ⚙️ Execution Options")
                
                col_opt1, col_opt2, col_opt3,col_opt4 = st.columns(4)
                
                with col_opt1:
                    single_historical = st.checkbox(
                        "📦 Historical Load",
                        value=True,
                        help="Load existing data from S3 before starting CDC"
                    )
                
                with col_opt2:
                    single_snowpipe = st.checkbox(
                        "🔄 Enable Snowpipe",
                        value=False,
                        help="Create Snowpipe for automatic data ingestion"
                    )
                with col_opt3:
                    single_stream = st.checkbox(
                        "🔄 Enable stream",
                        value=False,
                        help="Create stream for cdc"
                    )
                
                with col_opt4:
                    single_task = st.checkbox(
                        "⚡ Enable Task",
                        value=True,
                        help="Create and start task for CDC processing"
                    )
                
                single_description = st.text_area(
                    "📝 Description (Optional)",
                    placeholder="Enter a description for this CDC configuration...",
                    help="Optional description for documentation purposes"
                )
                
                st.markdown("---")
                
                col_submit1, col_submit2 = st.columns([1, 3])
                
                with col_submit1:
                    submit_single = st.form_submit_button("🚀 Execute Now", use_container_width=True, type="primary")
                
                with col_submit2:
                    st.markdown("""
                    <div style='padding: 10px; background-color: #fff3cd; border-radius: 5px; margin-top: 5px;'>
                        ⚠️ This will immediately execute CDC setup for the specified table
                    </div>
                    """, unsafe_allow_html=True)
            
            if submit_single:
                # Validation
                if not single_config_name:
                    st.error("❌ Configuration name is required!")
                elif not single_source_url:
                    st.error("❌ Source S3 URL is required!")
                elif not single_primary_key:
                    st.error("❌ Primary key is required!")
                else:
                    with st.spinner(f"🔄 Executing CDC for {single_config_name}..."):
                        try:
                            # Build configuration dictionary
                            single_config = {
                                "source_url": single_source_url.strip(),
                                "primary_key": single_primary_key.strip(),
                                "schema": f"{single_db}.{single_staging_schema}",
                                "gold_schema": f"{single_db}.{single_target_schema}",
                                "warehouse": single_warehouse.strip(),
                                "schedule": single_schedule.strip(),
                                "historical_copy": single_historical,
                                "snowpipe": single_snowpipe,
                                "stream": single_stream,
                                "task": single_task,
                                "file_format": single_file_format.strip(),
                                "description": single_description.strip()
                            }
                            
                            # Prepare full file format path
                            full_file_format = f"{st.session_state.config_db}.{st.session_state.config_schema}.{single_file_format.strip()}"
                            
                            # Execution log
                            execution_log = []
                            
                            st.info(f"🔄 Processing: {single_config_name}")
                            
                            # Execute CDC
                            run_cdc(session, single_config_name, single_config, full_file_format, execution_log)
                            
                            # Display results
                            st.markdown("---")
                            st.markdown("### 📊 Execution Summary")
                            
                            success_count = sum(1 for entry in execution_log if entry[1] == "Success")
                            failed_count = sum(1 for entry in execution_log if entry[1] == "Failed")
                            warning_count = sum(1 for entry in execution_log if entry[1] == "Warning")
                            
                            col_sum1, col_sum2, col_sum3, col_sum4 = st.columns(4)
                            with col_sum1:
                                st.metric("Total Steps", len(execution_log))
                            with col_sum2:
                                st.metric("✅ Successful", success_count)
                            with col_sum3:
                                st.metric("⚠️ Warnings", warning_count)
                            with col_sum4:
                                st.metric("❌ Failed", failed_count)
                            
                            st.markdown("### 📝 Detailed Execution Log")
                            for entry in execution_log:
                                status_color = "#28a745" if entry[1] == "Success" else ("#ffc107" if entry[1] == "Warning" else "#dc3545")
                                status_icon = "✅" if entry[1] == "Success" else ("⚠️" if entry[1] == "Warning" else "❌")
                                bg_color = "#e8f8f0" if entry[1] == "Success" else ("#fff3cd" if entry[1] == "Warning" else "#f8e8e8")
                                st.markdown(f"""
                                <div style="padding: 15px; margin: 10px 0; border-left: 4px solid {status_color}; background-color: {bg_color}; border-radius: 5px;">
                                    <strong>{status_icon} {entry[0]}</strong> ({entry[1]})<br>
                                    {entry[2]}
                                </div>
                                """, unsafe_allow_html=True)
                            
                            if failed_count == 0:
                                st.success(f"""
                                ✅ **CDC setup completed successfully for {single_config_name}!**
                                
                                **Created Objects:**
                                - Stage: `{single_config['schema']}.{single_source_url.rstrip('/').split('/')[-1]}_STAGE`
                                - Staging Table: `{single_config['schema']}.{single_source_url.rstrip('/').split('/')[-1]}_STAGING`
                                - Target Table: `{single_config['gold_schema']}.{single_source_url.rstrip('/').split('/')[-1]}`
                                - Stream: `{single_config['schema']}.{single_source_url.rstrip('/').split('/')[-1]}_STREAM`
                                {f"- Snowpipe: `{single_config['schema']}.{single_source_url.rstrip('/').split('/')[-1]}_PIPE`" if single_snowpipe else ""}
                                {f"- Task: `{single_config['schema']}.{single_source_url.rstrip('/').split('/')[-1]}_TASK`" if single_task else ""}
                                """)
                            else:
                                st.error(f"❌ CDC setup completed with {failed_count} error(s). Please review the execution log above.")
                            
                        except Exception as e:
                            st.error(f"❌ Execution failed: {str(e)}")
                            log_action("Single Table Execution", "Failed", str(e), single_config_name)
    
        with tab7:
            st.markdown("<h2 class='header'>📊 CDC Monitoring</h2>", unsafe_allow_html=True)
            
            if not st.session_state.cdc_configs:
                st.info("ℹ️ No configurations available. Please create a configuration first.")
            else:
                selected_config = st.selectbox(
                    "📋 Select Configuration to Monitor",
                    options=list(st.session_state.cdc_configs.keys()),
                    index=0 if not st.session_state.active_config else 
                          list(st.session_state.cdc_configs.keys()).index(st.session_state.active_config)
                )
                config = st.session_state.cdc_configs[selected_config]
                
                base_name = config["source_url"].rstrip('/').split('/')[-1]
                expected_objects = {
                    "stage": f"{base_name}_STAGE",
                    "staging_table": f"{base_name}_STAGING",
                    "target_table": base_name,
                    "stream": f"{base_name}_STREAM",
                    "task": f"{base_name}_TASK",
                    "pipe": f"{base_name}_PIPE"
                }
                
                st.markdown("### 🔍 CDC Object Status")
                
                # Stage Status
                try:
                    stage = session.sql(f"""
                        SELECT stage_name, stage_type, created
                        FROM {st.session_state.config_db}.INFORMATION_SCHEMA.STAGES
                        WHERE stage_schema = '{config['schema'].split('.')[-1]}'
                        AND stage_name = '{expected_objects['stage']}'
                    """).collect()
                    
                    if stage:
                        st.markdown(f"""
                        <div class='object-card'>
                            <div class='object-title'>📦 Stage: {config['schema']}.{expected_objects['stage']}</div>
                            <p><strong>Status:</strong> <span class='status-active'>✅ Active</span></p>
                            <p><strong>Type:</strong> {stage[0]['STAGE_TYPE']}</p>
                            <p><strong>Created:</strong> {stage[0]['CREATED']}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                        <div class='object-card'>
                            <div class='object-title'>📦 Stage: {config['schema']}.{expected_objects['stage']}</div>
                            <p><strong>Status:</strong> <span class='status-inactive'>❌ Not Found</span></p>
                        </div>
                        """, unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"❌ Error checking stage: {str(e)}")
                
                # Staging Table Status
                try:
                    table = session.sql(f"""
                        SELECT table_name, row_count, created
                        FROM {st.session_state.config_db}.INFORMATION_SCHEMA.TABLES
                        WHERE table_schema = '{config['schema'].split('.')[-1]}'
                        AND table_name = '{expected_objects['staging_table']}'
                    """).collect()
                    
                    if table:
                        st.markdown(f"""
                        <div class='object-card'>
                            <div class='object-title'>📋 Staging Table: {config['schema']}.{expected_objects['staging_table']}</div>
                            <p><strong>Status:</strong> <span class='status-active'>✅ Active</span></p>
                            <p><strong>Rows:</strong> {table[0]['ROW_COUNT']:,}</p>
                            <p><strong>Created:</strong> {table[0]['CREATED']}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                        <div class='object-card'>
                            <div class='object-title'>📋 Staging Table: {config['schema']}.{expected_objects['staging_table']}</div>
                            <p><strong>Status:</strong> <span class='status-inactive'>❌ Not Found</span></p>
                        </div>
                        """, unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"❌ Error checking staging table: {str(e)}")
                
                # Target Table Status
                try:
                    table = session.sql(f"""
                        SELECT table_name, row_count, created
                        FROM {st.session_state.config_db}.INFORMATION_SCHEMA.TABLES
                        WHERE table_schema = '{config['gold_schema'].split('.')[-1]}'
                        AND table_name = '{expected_objects['target_table']}'
                    """).collect()
                    
                    if table:
                        st.markdown(f"""
                        <div class='object-card'>
                            <div class='object-title'>🎯 Target Table: {config['gold_schema']}.{expected_objects['target_table']}</div>
                            <p><strong>Status:</strong> <span class='status-active'>✅ Active</span></p>
                            <p><strong>Rows:</strong> {table[0]['ROW_COUNT']:,}</p>
                            <p><strong>Created:</strong> {table[0]['CREATED']}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                        <div class='object-card'>
                            <div class='object-title'>🎯 Target Table: {config['gold_schema']}.{expected_objects['target_table']}</div>
                            <p><strong>Status:</strong> <span class='status-inactive'>❌ Not Found</span></p>
                        </div>
                        """, unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"❌ Error checking target table: {str(e)}")
                
                # Stream Status
                try:
                    stream = session.sql(f"""
                        SELECT stream_name, table_name, owner, created
                        FROM {st.session_state.config_db}.INFORMATION_SCHEMA.STREAMS
                        WHERE stream_schema = '{config['schema'].split('.')[-1]}'
                        AND stream_name = '{expected_objects['stream']}'
                    """).collect()
                    
                    if stream:
                        st.markdown(f"""
                        <div class='object-card'>
                            <div class='object-title'>🌊 Stream: {config['schema']}.{expected_objects['stream']}</div>
                            <p><strong>Status:</strong> <span class='status-active'>✅ Active</span></p>
                            <p><strong>Source Table:</strong> {stream[0]['TABLE_NAME']}</p>
                            <p><strong>Created:</strong> {stream[0]['CREATED']}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                        <div class='object-card'>
                            <div class='object-title'>🌊 Stream: {config['schema']}.{expected_objects['stream']}</div>
                            <p><strong>Status:</strong> <span class='status-inactive'>❌ Not Found</span></p>
                        </div>
                        """, unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"❌ Error checking stream: {str(e)}")
                
                # Snowpipe Status
                if config.get("snowpipe", False):
                    try:
                        pipe = session.sql(f"""
                            SELECT pipe_name, pipe_owner, created
                            FROM {st.session_state.config_db}.INFORMATION_SCHEMA.PIPES
                            WHERE pipe_schema = '{config['schema'].split('.')[-1]}'
                            AND pipe_name = '{expected_objects['pipe']}'
                        """).collect()
                        
                        if pipe:
                            st.markdown(f"""
                            <div class='object-card'>
                                <div class='object-title'>🔄 Snowpipe: {config['schema']}.{expected_objects['pipe']}</div>
                                <p><strong>Status:</strong> <span class='status-active'>✅ Active</span></p>
                                <p><strong>Owner:</strong> {pipe[0]['PIPE_OWNER']}</p>
                                <p><strong>Created:</strong> {pipe[0]['CREATED']}</p>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Show pipe status details
                            if st.button("📊 View Snowpipe Execution History", key=f"pipe_history_{selected_config}"):
                                try:
                                    pipe_history = session.sql(f"""
                                        SELECT *
                                        FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
                                            PIPE_NAME => '{config['schema']}.{expected_objects['pipe']}',
                                            DATE_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
                                        ))
                                        ORDER BY START_TIME DESC
                                        LIMIT 20
                                    """).to_pandas()
                                    
                                    if not pipe_history.empty:
                                        st.dataframe(pipe_history, use_container_width=True)
                                    else:
                                        st.info("No execution history found for the last 7 days")
                                except Exception as e:
                                    st.warning(f"Could not retrieve pipe history: {str(e)}")
                        else:
                            st.markdown(f"""
                            <div class='object-card'>
                                <div class='object-title'>🔄 Snowpipe: {config['schema']}.{expected_objects['pipe']}</div>
                                <p><strong>Status:</strong> <span class='status-inactive'>❌ Not Found</span></p>
                            </div>
                            """, unsafe_allow_html=True)
                    except Exception as e:
                        st.error(f"❌ Error checking Snowpipe: {str(e)}")
                
                # Task Status
                if config["task"]:
                    try:
                        task = session.sql(f"""
                            SELECT task_name, warehouse_name, schedule, state, created
                            FROM {st.session_state.config_db}.INFORMATION_SCHEMA.TASKS
                            WHERE task_schema = '{config['schema'].split('.')[-1]}'
                            AND task_name = '{expected_objects['task']}'
                        """).collect()
                        
                        if task:
                            status_class = "status-active" if task[0]['STATE'] == 'started' else "status-warning"
                            status_icon = "✅" if task[0]['STATE'] == 'started' else "⚠️"
                            st.markdown(f"""
                            <div class='object-card'>
                                <div class='object-title'>⚡ Task: {config['schema']}.{expected_objects['task']}</div>
                                <p><strong>Status:</strong> <span class='{status_class}'>{status_icon} {task[0]['STATE'].capitalize()}</span></p>
                                <p><strong>Warehouse:</strong> {task[0]['WAREHOUSE_NAME']}</p>
                                <p><strong>Schedule:</strong> {task[0]['SCHEDULE']}</p>
                                <p><strong>Created:</strong> {task[0]['CREATED']}</p>
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""
                            <div class='object-card'>
                                <div class='object-title'>⚡ Task: {config['schema']}.{expected_objects['task']}</div>
                                <p><strong>Status:</strong> <span class='status-inactive'>❌ Not Found</span></p>
                            </div>
                            """, unsafe_allow_html=True)
                    except Exception as e:
                        st.error(f"❌ Error checking task: {str(e)}")
                
                # Stream Data Preview
                st.markdown("### 🔍 Stream Data Preview")
                try:
                    stream_data = session.sql(f"""
                        SELECT COUNT(*) as record_count
                        FROM {config['schema']}.{expected_objects['stream']}
                    """).collect()
                    
                    if stream_data[0]['RECORD_COUNT'] > 0:
                        st.markdown(f"""
                        <div class='info-box'>
                            Stream contains <strong>{stream_data[0]['RECORD_COUNT']:,}</strong> record(s) waiting to be processed.
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if st.button("👁️ Preview Stream Data", use_container_width=True):
                            preview_data = session.sql(f"""
                                SELECT * 
                                FROM {config['schema']}.{expected_objects['stream']}
                                LIMIT 10
                            """).to_pandas()
                            st.dataframe(preview_data, use_container_width=True)
                    else:
                        st.markdown("""
                        <div class='success-box'>
                            ✅ Stream is currently empty (no new changes to process).
                        </div>
                        """, unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"❌ Error checking stream data: {str(e)}")
        
        with tab8:
            st.markdown("<h2 class='header'>📝 Execution Logs</h2>", unsafe_allow_html=True)
            
            if not st.session_state.log_entries:
                st.info("ℹ️ No log entries available yet.")
            else:
                col1, col2, col3 = st.columns(3)
                with col1:
                    filter_config = st.selectbox(
                        "🔍 Filter by Configuration",
                        options=["All"] + list(st.session_state.cdc_configs.keys())
                    )
                with col2:
                    filter_status = st.selectbox(
                        "📊 Filter by Status",
                        options=["All", "Success", "Failed", "Warning"]
                    )
                with col3:
                    if st.button("🗑️ Clear All Logs", use_container_width=True):
                        st.session_state.log_entries = []
                        st.rerun()
                
                filtered_logs = st.session_state.log_entries
                if filter_config != "All":
                    filtered_logs = [log for log in filtered_logs if log["config"] == filter_config]
                if filter_status != "All":
                    filtered_logs = [log for log in filtered_logs if log["status"].lower() == filter_status.lower()]
                
                st.markdown(f"### 📋 Showing {len(filtered_logs)} log entries")
                
                for log in reversed(filtered_logs):
                    if log["status"] == "Success":
                        st.markdown(f"""
                        <div class='success-box'>
                            <strong>✅ {log['timestamp']} - {log['action']}</strong><br>
                            <em>Configuration:</em> {log['config'] or 'N/A'}<br>
                            {log['message']}
                        </div>
                        """, unsafe_allow_html=True)
                    elif log["status"] == "Failed":
                        st.markdown(f"""
                        <div class='error-box'>
                            <strong>❌ {log['timestamp']} - {log['action']}</strong><br>
                            <em>Configuration:</em> {log['config'] or 'N/A'}<br>
                            {log['message']}
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                        <div class='warning-box'>
                            <strong>⚠️ {log['timestamp']} - {log['action']}</strong><br>
                            <em>Configuration:</em> {log['config'] or 'N/A'}<br>
                            {log['message']}
                        </div>
                        """, unsafe_allow_html=True)
                
                # Export logs option
                if filtered_logs:
                    st.markdown("---")
                    st.markdown("### 📥 Export Logs")
                    
                    logs_df = pd.DataFrame(filtered_logs)
                    csv = logs_df.to_csv(index=False).encode('utf-8')
                    
                    st.download_button(
                        label="📥 Download Logs as CSV",
                        data=csv,
                        file_name=f"cdc_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )

if st.session_state.setup_complete:
    main()