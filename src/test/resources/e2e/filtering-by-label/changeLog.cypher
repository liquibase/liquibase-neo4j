-- liquibase formatted cypher

-- changeset fbiville:my-movie-init labels:mandatory
CREATE (:Movie {title: 'My Life', genre: 'Comedy'});

-- changeset fbiville:oblivion labels:!mandatory
MATCH (n) DETACH DELETE n
