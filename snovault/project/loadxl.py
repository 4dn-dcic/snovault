# Loadxl related functions which may be overriden by an implementing app,
# e.g. Foursight or CGAP portal, using the dcicutils project_utils mechanism.

class SnovaultProjectLoadxl:
    def loadxl_order(self):
        return []
