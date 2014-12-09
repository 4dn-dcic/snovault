from ..auditor import (
    AuditFailure,
    audit_checker,
)


@audit_checker('antibody_characterization')
def audit_antibody_characterization_review(value, system):
    '''
    Make sure that biosample terms are in ontology
    for each characterization_review.
    '''
    if (value['status'] in ['not reviewed', 'not submitted for review by lab', 'deleted', 'in progress']):
        return

    if 'secondary_characterization_method' in value:
        return

    if value['characterization_reviews']:
        ontology = system['registry']['ontology']
        for review in value['characterization_reviews']:

            term_id = review['biosample_term_id']
            term_name = review['biosample_term_name']

            if term_id.startswith('NTR:'):
                detail = '{} - {}'.format(term_id, term_name)
                raise AuditFailure('NTR', detail, level='DCC_ACTION')

            if term_id not in ontology:
                raise AuditFailure('term id not in ontology', term_id, level='DCC_ACTION')

            ontology_term_name = ontology[term_id]['name']
            if ontology_term_name != term_name and term_name not in ontology[term_id]['synonyms']:
                detail = '{} - {} - {}'.format(term_id, term_name, ontology_term_name)
                raise AuditFailure('term name mismatch', detail, level='ERROR')


@audit_checker('antibody_characterization')
def audit_antibody_characterization_unique_reviews(value, system):
    '''
    Make sure primary characterizations have unique lane, biosample_term_id and
    organism combinations for characterization reviews
    '''
    if(value['status'] in ['deleted', 'not submitted for review by lab', 'in progress', 'not reviewed']):
        return

    if 'secondary_characterization_method' in value:
        return

    unique_reviews = set()
    for review in value['characterization_reviews']:
        lane = review['lane']
        term_id = review['biosample_term_id']
        organism = review['organism']
        review_lane = frozenset([lane, term_id, organism])
        if review_lane not in unique_reviews:
            unique_reviews.add(review_lane)
        else:
            detail = '{} - {} - {}'.format(lane, term_id, organism)
            raise AuditFailure('duplicate lane review', detail, level='ERROR')


@audit_checker('antibody_characterization')
def audit_antibody_characterization_target(value, system):
    '''
    Make sure that target in characterization
    matches target of antibody
    '''
    antibody = value['characterizes']
    target = value['target']
    if 'recombinant protein' in target['investigated_as']:
        prefix = target['label'].split('-')[0]
        unique_antibody_target = set()
        unique_investigated_as = set()
        for antibody_target in antibody['targets']:
            label = antibody_target['label']
            unique_antibody_target.add(label)
            for investigated_as in antibody_target['investigated_as']:
                unique_investigated_as.add(investigated_as)
        if 'tag' not in unique_investigated_as:
            detail = '{} is not to tagged protein'.format(antibody['@id'])
            raise AuditFailure('not tagged antibody', detail, level='ERROR')
        else:
            if prefix not in unique_antibody_target:
                detail = '{} not found in target for {}'.format(prefix, antibody['@id'])
                raise AuditFailure('tag target mismatch', detail, level='ERROR')
    else:
        target_matches = False
        for antibody_target in antibody['targets']:
            if target['name'] == antibody_target.get('name'):
                target_matches = True
        if not target_matches:
            detail = '{} not found in target for {}'.format(target['name'], antibody['@id'])
            raise AuditFailure('target mismatch', detail, level='ERROR')


@audit_checker('antibody_characterization')
def audit_antibody_characterization_status(value, system):
    '''
    Make sure the lane_status matches
    the characterization status
    '''
    if 'secondary_characterization_method' in value:
        return

    if(value['status'] in ["deleted", "not submitted for review by lab", 'in progress', 'not reviewed']):
        if 'characterization_reviews' in value:
            '''If any of these statuses, we shouldn't have characterization_reviews'''
            detail = 'status: {} is incompatible with having characterization_reviews'.format(value['status'])
            raise AuditFailure('unexpected characterization_reviews', detail, level='WARNING')
        else:
            return

    '''Check each of the lane_statuses in characterization_reviews for an appropriate match'''
    has_compliant_lane = False
    is_pending = False
    if value['status'] == 'pending dcc review':
        is_pending = True
    for lane in value['characterization_reviews']:
        if (is_pending and lane['lane_status'] != 'pending dcc review') or (not is_pending and lane['lane_status'] == 'pending dcc review'):
            detail = 'lane_status: {} is incompatible with pending dcc review'.format(lane['lane_status'])
            raise AuditFailure('char/lane status mismatch', detail, level='WARNING')
            continue

        if lane['lane_status'] == 'compliant':
            has_compliant_lane = True

    if has_compliant_lane and value['status'] != 'compliant':
        detail = 'lane_status: {} is incompatible with char status: {}'.format(lane['lane_status'], value['status'])
        raise AuditFailure('char/lane status mismatch', detail, level='DCC_ACTION')


@audit_checker('antibody_characterization')
def audit_antibody_characterization_method_allowed(value, system):
    '''Warn if a lab submits an ENCODE3 characterization if the method is not yet approved by the standards document.'''
    if 'primary_characterization_method' in value:
        return

    target = value['target']
    is_histone = False
    if 'histone modification' in target['investigated_as']:
        is_histone = True

    if ('award' not in value) or (value['award'].get('rfa') != 'ENCODE3'):
        return

    secondary = value['secondary_characterization_method']
    if (secondary == 'motif enrichment') or (is_histone and secondary == 'ChIP-seq comparison'):
        detail = '{} is not an approved secondary characterization_method according to the current standards'.format(value['secondary_characterization_method'])
        raise AuditFailure('unapproved char method', detail, level='NOT_COMPLIANT')
