class SnovaultProjectSchemaViews:

    def get_submittable_schema_names(self):
        # optional project dependent list of schema names in snake case
        return []
    
    def get_prop_for_submittable_items(self):
        # optional property name when if found in a schema indicates it is submittable
        return None
    
    def get_properties_for_exclusion(self):
        # optional properties that should be omitted from submittable fields
        return []
    
    def get_properties_for_inclusion(self):
        # optional properties that should be included as submittable - trumps exclusion or other criteria
        return []
    
    def get_attributes_for_exclusion(self):
        # optional attributes of a property that if found exclude the property
        # eg. {'permission':['restricted_fields'], 'calculateProperty': [True]}
        return {}

    def get_attributes_for_inclusion(self):
        # optional attribute that will cause the property to be included - trumps other criteria at attribute level
        return {}