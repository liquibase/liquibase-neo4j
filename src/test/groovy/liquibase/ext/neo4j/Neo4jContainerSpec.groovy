package liquibase.ext.neo4j

import liquibase.database.Database
import liquibase.database.DatabaseFactory
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Neo4jContainer
import spock.lang.Shared
import spock.lang.Specification

import java.time.ZoneId
import java.util.logging.LogManager

import static liquibase.ext.neo4j.DockerNeo4j.dockerTag

abstract class Neo4jContainerSpec extends Specification {
   static {
      LogManager.getLogManager().reset()
   }

   static final String PASSWORD = "s3cr3t"

   static final TIMEZONE = ZoneId.of("Europe/Paris")

   @Shared
   GenericContainer<Neo4jContainer> neo4jContainer = DockerNeo4j.container(PASSWORD, TIMEZONE, neo4jImageVersion())
           .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")

   @Shared
   CypherRunner queryRunner

   @Shared
   Database database

   def setupSpec() {
      neo4jContainer.start()
      queryRunner = new CypherRunner(
            GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", PASSWORD)),
              dockerTag())
      database = DatabaseFactory.instance.openDatabase(
              "jdbc:neo4j:${neo4jContainer.getBoltUrl()}",
              "neo4j",
              PASSWORD,
              null,
              null
      )
   }

   def cleanupSpec() {
      database.close()
      queryRunner.close()
      neo4jContainer.stop()
   }

   def cleanup() {
      queryRunner.run("MATCH (n) DETACH DELETE n")
   }

   protected neo4jImageVersion() {
      return dockerTag()
   }
}
