import os
import xmltodict
import requests
import rdflib
import girder_client
import urllib.parse

gc = girder_client.GirderClient(apiUrl='https://girder.wholetale.org/api/v1')
gc.authenticate(apiKey=os.environ['WT_GIRDER_APIKEY'])
DATAONE_COLL_ID = '57fc1a1986ed1d000173b463'


def DataONE_url(suffix, api=2):
    return 'https://cn.dataone.org/cn/v{}/{}/'.format(api, suffix)


def ingest_urn(current_urn):
    print("Ingesting {}".format(current_urn))
    g = rdflib.Graph()
    g.parse(DataONE_url('object', 1) + current_urn, format='xml')

    docBy_m = rdflib.term.URIRef('http://purl.org/spar/cito/isDocumentedBy')
    agg_m = rdflib.term.URIRef(
        'http://www.openarchives.org/ore/terms/isAggregatedBy')
    agg_cur = rdflib.term.URIRef(
        'https://cn.dataone.org/cn/v1/resolve/{}#aggregation'.format(current_urn))

    all_urns = set(list(g.subjects(agg_m, agg_cur)))

    meta_docs = list(set([_[-1] for _ in list(g.subject_objects(docBy_m))]))

    for doc in meta_docs:
        doc_url = urllib.parse.unquote(doc.toPython())
        r = requests.get(doc_url)
        metadata = xmltodict.parse(r.content, process_namespaces=True)

        doi_set = set(list(g.subjects(docBy_m, doc)))
        remaining_urns = (all_urns - doi_set) - set([doc])
        # recurse over remaining_urns
        for urn in remaining_urns:
            urn_base = urllib.parse.unquote(os.path.basename(urn.toPython()))
            print("Recursing into {}".format(urn_base))
            ingest_urn(urn_base)
        print("Back to ingesting {}".format(current_urn))

        namespace = list(metadata.keys())[0]
        meta = metadata[namespace]
        params = {'parentType': 'collection', 'parentId': DATAONE_COLL_ID,
                  'name': meta['@packageId']}
        gc_folder = gc.get('folder', parameters=params)
        if gc_folder:
            gc_folder = gc_folder[0]
        else:
            gc_folder = gc.post('folder', parameters=params)
        params = {
            'description': '## {}\n\n{}'.format(meta['dataset']['title'],
                                                meta['dataset']['abstract'])}
        gc.put('folder/%s' % gc_folder['_id'], parameters=params)
        folder_meta = dict((k, meta['dataset'][k])
                           for k in ('creator', 'pubDate', 'keywordSet'))
        folder_meta['doi'] = doc_url.split('doi:')[-1]
        gc.addMetadataToFolder(gc_folder['_id'], folder_meta)

        files_meta = [_['entityName'] for _ in meta['dataset']['otherEntity']]
        for subject in list(g.subjects(docBy_m, doc)):
            file_url = urllib.parse.unquote(subject.toPython())
            meta_url = DataONE_url('meta', api=2) + os.path.basename(file_url)
            r = requests.get(meta_url, allow_redirects=False)
            data = xmltodict.parse(r.content, process_namespaces=True)
            data_nm = list(data.keys())[0]
            try:
                file_name = data[data_nm]['fileName']
                file_size = data[data_nm]['size']
            except KeyError:
                # why does it happen?!
                print("file_url = {}".format(file_url))
                continue
            file_meta = None
            for fn in (file_name, os.path.splitext(file_name)[0]):
                try:
                    ind = files_meta.index(fn)
                    file_meta = meta['dataset']['otherEntity'][ind]
                except ValueError:
                    continue

            if file_meta is None:
                print("Something went wrong, catch this....")

            params = {'parentType': 'folder', 'parentId': gc_folder['_id'],
                      'linkUrl': file_url, 'name': file_name,
                      'size': int(file_size)}
            gc_file = gc.post('file', parameters=params)
            if file_meta is not None:
                gc.addMetadataToItem(gc_file['itemId'], file_meta)


params = {
    'q': 'id:"doi:10.5063/F1Z899CZ"',
    'fl': ('id,seriesId,fileName,resourceMap,formatType,formatId,read_count_i'
           'obsoletedBy,isDocumentedBy,documents,title,origin,size'
           'pubDate,dateUploaded,datasource,isAuthorized,isPublic'),
    'wt': 'json'
}
r = requests.get('https://search.dataone.org/cn/v2/query/solr/', params=params)
search = r.json()

# TODO check how many docs has been found
document = search['response']['docs'][0]
resource_map = document['resourceMap'][0]  # TODO check if exist

ingest_urn(resource_map)
