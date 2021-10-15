
# ACS attributes

prop = {}
prop['schemas'] = {}

schemas = [f'tiger201{x}' for x in range(3,10)]
for schema in schemas:
    prop['schemas'][schema] = {}

prop['schemas']['tiger2013']['tables'] = [
    'aiannh','aits','anrc','bg','cbsa','cd','census_geo_containment','census_name_lookup','cnecta','concity','county',
    'cousub','csa','elsd','metdiv','necta','nectadiv','place','puma','scsd','sldl','sldu','state','submcd','tabblock',
    'tbg','tract','ttract','uac','unsd','zcta5']

prop['schemas']['tiger2014']['tables'] = [
    'aiannh','aits','anrc','bg','cbsa','cd','census_geo_containment','census_name_lookup','cnecta','concity','county',
    'cousub','csa','elsd','metdiv','necta','nectadiv','place','puma','scsd','sldl','sldu','state','submcd','tabblock',
    'tbg','tract','ttract','uac','unsd','zcta5']

prop['schemas']['tiger2015']['tables'] = [
    'aiannh','anrc','bg','cbsa','cd','census_geo_containment','census_name_lookup','cnecta','concity','county','cousub',
    'csa','elsd','metdiv','necta','nectadiv','place','puma','scsd','sldl','sldu','state','tabblock','tbg','tract','ttract',
    'uac','unsd','zcta5']

prop['schemas']['tiger2016']['tables'] = [
    'aiannh','aitsn','anrc','bg','cbsa','cd','census_geo_containment','census_name_lookup','cnecta','concity','county',
    'cousub','csa','elsd','metdiv','necta','nectadiv','place','puma','scsd','sldl','sldu','state','submcd','tbg','tract',
    'ttract','uac','unsd','zcta5']

prop['schemas']['tiger2017']['tables'] = [
    'aiannh','aitsn','anrc','bg','cbsa','cd','census_geo_containment','census_name_lookup','cnecta','concity','county',
    'cousub','csa','elsd','metdiv','necta','nectadiv','place','puma','scsd','sldl','sldu','state','submcd','tbg','tract',
    'ttract','uac','unsd','zcta5']

prop['schemas']['tiger2018']['tables'] = [
    'aiannh','aiannh250','aiannh252','aiannh254','aitsn','anrc','bg','cbsa','cd','census_geo_containment','census_name_lookup',
    'cnecta','concity','county','cousub','csa','elsd','metdiv','necta','nectadiv','place','puma','scsd','sldl','sldu','state',
    'submcd','tbg','tract','ttract','uac','unsd','zcta5']

prop['schemas']['tiger2019']['tables'] = [
    'aiannh','aiannh250','aiannh252','aiannh254','aitsn','anrc','bg','cbsa','cd','census_geo_containment','census_name_lookup',
    'cnecta','concity','county','cousub','csa','elsd','metdiv','necta','nectadiv','place','puma','scsd','sldl','sldu','state',
    'submcd','tbg','tract','ttract','uac','unsd','zcta5']
