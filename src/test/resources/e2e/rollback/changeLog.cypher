-- liquibase formatted cypher

-- changeset fbiville:my-movie-init
CREATE (:Movie {title: 'My Life', genre: 'Comedy'});

-- changeset fbiville:translate
MATCH (m:Movie {title: 'My Life'}) SET m.genre = 'Comédie'
-- rollback MATCH (m:Movie {title: 'My Life'}) SET m.genre = 'Comedy'
