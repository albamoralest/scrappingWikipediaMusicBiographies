from SPARQLWrapper import SPARQLWrapper, JSON
sparql = SPARQLWrapper("https://dbpedia.org/sparql/")

for i in range(25):
    query = """
prefix skos: <http://www.w3.org/2004/02/skos/core#>
prefix dbo: <http://dbpedia.org/ontology/>
select distinct ?s ?id ?birth
where {
?s <http://purl.org/dc/terms/subject>/skos:broader{0,5} <http://dbpedia.org/resource/Category:People> .
?s <http://purl.org/dc/terms/subject>/skos:broader{0,5} <http://dbpedia.org/resource/Category:Music_people> .
?s dbo:birthDate ?birth .
?s dbo:wikiPageID ?id
FILTER (?birth >= "1700-01-01"^^xsd:date && ?birth <= "1947-12-31"^^xsd:date)
}
limit 1000
OFFSET """ + str(i) + "000"
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    l = []
    for re in results["results"]["bindings"]:
        l.append("{} {} {}".format(re["s"], ["id"], re["birth"]))
    with open("listBiographies/results.tsv", "a") as f:
        for x in l:
            f.write(x + "\n")