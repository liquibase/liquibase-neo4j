-- liquibase formatted cypher

-- changeset fbiville:my-movie-init contextFilter:mandatory
CREATE (:Movie {title: 'My Life', genre: 'Comedy'});

-- changeset fbiville:oblivion contextFilter:!mandatory
MATCH (n) DETACH DELETE n
