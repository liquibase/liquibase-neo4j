-- liquibase formatted sql

-- changeset fbiville:my-movie-init runInTransaction:false
CALL { CREATE (:Movie {title: 'My Life', genre: 'Comedy'}) } IN TRANSACTIONS
