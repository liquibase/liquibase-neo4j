MATCH (m:Movie {title: 'My Life'})
MATCH (a:Person {name: 'Hater'})
MATCH (a)-[r:RATED {rating: 0}]->(m) SET r.rating = 5