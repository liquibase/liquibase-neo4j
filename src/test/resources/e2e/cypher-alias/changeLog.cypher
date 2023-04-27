-- liquibase formatted cypher

-- changeset fbiville:my-movie-init
CREATE (:Movie {title: 'My Life', genre: 'Comedy'});
