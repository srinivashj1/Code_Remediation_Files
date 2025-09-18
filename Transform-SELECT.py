import re
import pandas as pd
from functools import lru_cache
from typing import Union

# --- Load Mappings with Error Handling ---
@lru_cache(maxsize=1)
def load_mappings_cached():
    EXCEL_URL = "https://github.com/srinivashj1/Code_Remediation/raw/main/Select_Queries_Mapping.xlsx"
    MAPPING_SHEET = "CRM_S4_TABLE_FIELDS_MAPPING"
    try:
        df = pd.read_excel(EXCEL_URL, sheet_name=MAPPING_SHEET)
    except Exception as e:
        print(f"Error loading mapping file: {e}")
        return {}, {}
    table_map = dict(zip(df["CRM_TABNAME"].astype(str).str.upper(), df["S4_TABNAME"].astype(str)))
    field_map = {}
    for _, row in df.iterrows():
        crm_tabname = str(row["CRM_TABNAME"]).upper()
        crm_fieldname = str(row["CRM_FIELDNAME"]).upper()
        s4_tabname = str(row["S4_TABNAME"])
        s4_fieldname = str(row["S4_FIELDNAME"])
        field_map[(crm_tabname, crm_fieldname)] = (s4_tabname, s4_fieldname)
    return table_map, field_map

table_mapping, field_mapping = load_mappings_cached()

REMOVE_JOIN_TABLES = {
    "CRMD_LINK", "CRMD_SALES", "CRMD_BILLING", "CRMD_ORGMAN", "CRMD_PRICING", "CRMD_PRICING_I"
}

def transform_query(source_query: str) -> str:
    query = re.sub(r'\s+', ' ', source_query.strip())
    # Special logic for CRMD_CUSTOMER_H join with CRMD_ORDERADM_H
    # If "FROM crmd_orderadm_h AS a INNER JOIN crmd_customer_h AS b ON b~guid = a~guid"
    # replace FROM+JOIN with FROM CRMS4D_SERV_H
    special_join_pattern = (
        r'FROM\s+(crmd_orderadm_h)\s+AS\s+(\w+)\s+'
        r'INNER\s+JOIN\s+(crmd_customer_h)\s+AS\s+(\w+)\s+ON\s+\4~guid\s*=\s*\2~guid'
    )

    match = re.search(special_join_pattern, query, re.IGNORECASE)
    if match:
        # Replace FROM ... JOIN ... ON ... with FROM CRMS4D_SERV_H
        query = re.sub(special_join_pattern,
                       'FROM CRMS4D_SERV_H ',
                       query, flags=re.IGNORECASE)
        main_table = "CRMD_ORDERADM_H"
        join_table = "CRMD_CUSTOMER_H"
        s4_tab = "CRMS4D_SERV_H"
        alias_map = {match.group(2).upper(): main_table, match.group(4).upper(): join_table}
    else:
        # Normal logic: get main table and alias
        from_match = re.search(r'FROM\s+(\w+)(?:\s+AS\s+(\w+))?', query, re.IGNORECASE)
        if not from_match:
            return query
        main_table = from_match.group(1).upper()
        main_alias = from_match.group(2).upper() if from_match.group(2) else None
        s4_tab = table_mapping.get(main_table, main_table)
        alias_map = {main_alias: main_table} if main_alias else {}

    # Remove joins for specified tables
    def join_remover(match):
        table = match.group(1).upper()
        if table in REMOVE_JOIN_TABLES or table == "CRMD_CUSTOMER_H":
            return ''
        return match.group(0)

    query = re.sub(
        r'INNER JOIN\s+(\w+)\s+AS\s+\w+\s+ON\s+[^I]*?(?=(INNER JOIN|\bWHERE\b|$))',
        join_remover,
        query,
        flags=re.IGNORECASE
    )

    # Extract SELECT fields and aliases
    select_fields_match = re.search(r'SELECT (.*?) FROM', query, re.IGNORECASE)
    fields = []
    comma_in_select = False
    if select_fields_match:
        fields_str = select_fields_match.group(1).strip()
        if ',' in fields_str:
            comma_in_select = True
            fields = [f.strip() for f in fields_str.split(',')]
        else:
            fields = [f.strip() for f in fields_str.split() if f.strip()]
    else:
        fields_match = re.search(r'FIELDS (.*?)(WHERE|INTO|$)', query, re.IGNORECASE)
        if fields_match:
            fields_str = fields_match.group(1).strip().rstrip(',')
            if ',' in fields_str:
                comma_in_select = True
                fields = [f.strip() for f in fields_str.split(',')]
            else:
                fields = [f.strip() for f in fields_str.split() if f.strip()]

    fields = [f for f in fields if f.strip().upper() != 'SINGLE']

    # Map fields using Excel mapping
    def map_field(field):
        original_field = field
        if '~' in field:
            alias, fname = field.split('~', 1)
            alias = alias.strip().upper()
        else:
            fname = field
            alias = None

        fname = fname.strip().upper()

        if match:
            # Special join, resolve which CRM table by alias
            if alias and alias in alias_map:
                crm_tab = alias_map[alias]
            else:
                crm_tab = main_table
        else:
            crm_tab = main_table

        s4_tabname, s4_fieldname = field_mapping.get((crm_tab, fname), (None, None))
        if not s4_tabname:
            # Fallback to main S4 table and field name same as original
            s4_tabname = table_mapping.get(crm_tab, crm_tab)
            s4_fieldname = fname

        return f"{s4_fieldname} AS {fname}"

    transformed_fields = [map_field(f) for f in fields]

    # INTO clause
    into_clause = ''
    into_match = re.search(r'INTO\s+(TABLE\s+)?((@?DATA\([^)]+\))|(@?\w+))', query, re.IGNORECASE)
    if into_match:
        into_var = into_match.group(2)
        if into_match.group(1):
            into_clause = f'INTO TABLE {into_var}'
        else:
            into_clause = f'INTO {into_var}'

    # WHERE clause
    where_clause = ''
    where_match = re.search(r'WHERE (.*?)(FOR ALL ENTRIES|INTO|UP TO|$)', query, re.IGNORECASE)
    if where_match:
        where_cond = where_match.group(1).strip()
        # Try mapping fields in WHERE clause
        def map_where_field(matchw):
            left = matchw.group(1)
            op = matchw.group(2)
            right = matchw.group(3)
            # Try to resolve alias
            if '~' in left:
                alias, left_field = left.split('~', 1)
                alias = alias.strip().upper()
                left_field = left_field.strip().upper()
            else:
                left_field = left.strip().upper()
                alias = None

            if match:
                # Special join, resolve CRM table by alias
                crm_tab = alias_map.get(alias, main_table)
            else:
                crm_tab = main_table

            s4_tabname, s4_fieldname = field_mapping.get((crm_tab, left_field), (None, None))
            if not s4_fieldname:
                s4_fieldname = left_field
            return f"{s4_fieldname}{op}{right}"

        where_cond = re.sub(r'([@]?\w+(?:~\w+)?)(\s*(?:=|EQ|NE|<|>|LIKE|IN)\s*)([^,\)\s]+)', map_where_field, where_cond, flags=re.IGNORECASE)
        where_clause = f'WHERE {where_cond}'

    # FOR ALL ENTRIES
    fae_clause = ''
    fae_match = re.search(r'FOR ALL ENTRIES IN\s+(@?\w+)', query, re.IGNORECASE)
    if fae_match:
        fae_clause = f'FOR ALL ENTRIES IN {fae_match.group(1)}'

    # UP TO ... ROWS
    up_to_clause = ''
    up_to_match = re.search(r'UP TO (\w+) ROWS', query, re.IGNORECASE)
    if up_to_match:
        up_to_clause = f'UP TO {up_to_match.group(1)} ROWS'

    # Compose query
    parts = []
    if re.search(r'\bSINGLE\b', query, re.IGNORECASE):
        parts.append('SELECT SINGLE')
    else:
        parts.append('SELECT')

    if comma_in_select:
        parts.append(', '.join(transformed_fields))
    else:
        parts.append(' '.join(transformed_fields))
    parts.append(f'FROM {s4_tab}')

    if where_clause:
        parts.append(where_clause)
    if into_clause:
        parts.append(into_clause)
    if fae_clause:
        parts.append(fae_clause)
    if up_to_clause:
        parts.append(up_to_clause)

    transformed_query = ' '.join(parts)
    return transformed_query.strip()

def transform_abap_program_input(input_abap: Union[str, bytes]) -> str:
    """
    Accepts either a string (ABAP code) or a file path (str/bytes).
    Returns the remediated ABAP program.
    """
    # If file path, read file contents
    if isinstance(input_abap, str) and input_abap.endswith('.abap'):
        try:
            with open(input_abap, 'r', encoding='utf-8') as f:
                program_source = f.read()
        except Exception as e:
            print(f"Error reading ABAP file: {e}")
            return ''
    elif isinstance(input_abap, bytes):
        program_source = input_abap.decode('utf-8')
    else:
        program_source = input_abap
    return transform_abap_program(program_source)

def transform_abap_program(program_source: str) -> str:
    # Pattern to match ABAP SELECT blocks (including multiline)
    select_pattern = re.compile(
        r'(SELECT[\s\S]+?FROM[\s\S]+?(?:WHERE[\s\S]+?)?(?:FOR ALL ENTRIES IN[\s\S]+?)?(?:INTO[\s\S]+?)?(?:UP TO[\s\S]+?ROWS)?\s*\.)',
        re.IGNORECASE
    )
    def transform_match(match):
        select_query_block = match.group(0)
        select_query_one_line = ' '.join(select_query_block.replace('\n', ' ').split())
        select_query_no_period = select_query_one_line.rstrip('.')
        transformed = transform_query(select_query_no_period)
        return transformed + '.'
    transformed_program = select_pattern.sub(transform_match, program_source)
    return transformed_program

# For VSCode compatibility, add a main test block
if __name__ == "__main__":
    test_query = '''
    SELECT a~guid,
           a~object_type,
           a~object_id,
           b~zzpur_of_loan,
           b~zzsendtobid
     FROM crmd_orderadm_h AS a
     INNER JOIN crmd_customer_h AS b ON b~guid = a~guid
     INTO TABLE @lt_header
     FOR ALL ENTRIES IN @lt_filter_guids
     WHERE a~guid = @lt_filter_guids-table_line.
    '''
    print(transform_query(test_query))