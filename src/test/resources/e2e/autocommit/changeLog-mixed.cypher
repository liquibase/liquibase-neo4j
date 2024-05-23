-- liquibase formatted sql

-- changeset hindog:my-movie-init runInTransaction:false
CALL { CREATE (:Movie {title: 'My Life', genre: 'Comedy'}) } IN TRANSACTIONS

-- changeset hindog:my-movie-set-drama
MATCH (m:Movie {title: 'My Life', genre: 'Comedy'}) SET m.genre = 'Drama'

-- changeset hindog:my-movie-set-title runInTransaction:false
CALL { MATCH (m:Movie {title: 'My Life', genre: 'Drama'}) SET m.title = 'My Life 2' } IN TRANSACTIONS
