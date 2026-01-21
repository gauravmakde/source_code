import json
import xml.etree.ElementTree as ET

# Read the service account JSON key file
service_account_file = 'service account/SA_new_cf_nordstrom.json'

# Read the service account JSON key file
with open(service_account_file) as f:
    service_account_info = json.load(f)

# Escape quotes in the service account JSON for embedding in XML
sa_json_str = json.dumps(service_account_info)
sa_json_str = sa_json_str.replace('"', '&quot;')

# Create the XML structure
root = ET.Element('datasource')
connection = ET.SubElement(root, 'connection', {
    'class': 'google-bigquery',
    'dbname': 'cf-nordstrom',
    'server': 'https://www.googleapis.com/bigquery/v2:443'
})

customization = ET.SubElement(connection, 'connection-customization', {
    'class': 'generic',
    'enabled': 'true',
    'enabled-locally': 'true',
    'version': '10.0'
})

ET.SubElement(customization, 'vendor', {'name': 'bigquery'})
ET.SubElement(customization, 'driver', {'name': 'bigquery'})
ET.SubElement(customization, 'service', {'account': sa_json_str})

ET.SubElement(connection, 'relation', {
    'name': 'your_table_name',
    'type': 'table'
})

# Write to a TDS file
tree = ET.ElementTree(root)
tree.write('your_datasource.tds', encoding='utf-8', xml_declaration=True)
