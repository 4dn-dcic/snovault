class SnovaultProjectSchemaViews:

    def get_submittable_schema_names(self):
        return ['access_key']
    
    def get_prop_for_submittable_items(self):
        return None
    
    def get_properties_for_exclusion(self):
        return ['schema_version']
        #return []
    
    def get_properties_for_inclusion(self):
        return []
    
    def get_attributes_for_exclusion(self):
        return {'permission':['restricted_fields']}
        # return {}

    def get_attributes_for_inclusion(self):
        return {}