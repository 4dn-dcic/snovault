class SnovaultProjectSchemaViews:

    def get_submittable_item_names(self):
        """
        Optional project dependent list of item names in snake case format to include as submittable
        """
        return []
 
    def get_prop_for_submittable_items(self):
        """
        optional property name when if found as a top level property in a schema indicates it is submittable
        """
        return ""

    def get_properties_for_exclusion(self):
        """
        optional properties that should be omitted from submittable fields
        """
        return []

    def get_attributes_for_exclusion(self):
        """
        optional attributes of a property that if found exclude the property
        from submmittable propertie
        eg. {'permission':['restricted_fields'], 'calculatedProperty': [True]}
        """
        return {}
