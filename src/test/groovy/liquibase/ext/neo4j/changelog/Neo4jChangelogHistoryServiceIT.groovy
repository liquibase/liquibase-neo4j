package liquibase.ext.neo4j.changelog

import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Labels
import liquibase.change.Change
import liquibase.change.CheckSum
import liquibase.change.core.RawSQLChange
import liquibase.change.core.TagDatabaseChange
import liquibase.changelog.ChangeSet
import liquibase.changelog.DatabaseChangeLog
import liquibase.changelog.RanChangeSet
import liquibase.exception.DatabaseException
import liquibase.ext.neo4j.Neo4jContainerSpec
import org.neo4j.driver.exceptions.ClientException
import spock.lang.Requires

import java.time.ZonedDateTime

import static java.time.temporal.ChronoUnit.MINUTES
import static java.util.Collections.singletonList
import static liquibase.changelog.ChangeSet.ExecType.*
import static liquibase.changelog.ChangeSet.RunStatus.*
import static liquibase.ext.neo4j.DateUtils.date
import static liquibase.ext.neo4j.DateUtils.nowMinus
import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.MapUtils.containsAll
import static liquibase.ext.neo4j.ReflectionUtils.getField
import static liquibase.ext.neo4j.ReflectionUtils.setField
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.*

class Neo4jChangelogHistoryServiceIT extends Neo4jContainerSpec {

    def historyService = new Neo4jChangelogHistoryService()

    def setup() {
        historyService.setDatabase(database)
    }

    def cleanup() {
        queryRunner.dropUniqueConstraint(TAG_CONSTRAINT_NAME, "__LiquibaseTag", "tag")
        queryRunner.dropUniqueConstraint(LABEL_CONSTRAINT_NAME, "__LiquibaseLabel", "label")
        queryRunner.dropUniqueConstraint(CONTEXT_CONSTRAINT_NAME, "__LiquibaseContext", "context")
        if (enterpriseEdition()) {
            queryRunner.dropNodeKeyConstraint(CHANGE_SET_CONSTRAINT_NAME, "__LiquibaseChangeSet", "id", "author", "changeLog")
        }
    }

    def "creates constraints and changelog node upon initialization"() {
        when:
        historyService.init()

        then:
        def existingConstraints = queryRunner.listExistingConstraints()
        existingConstraints.findIndexOf { it.contains(":__LiquibaseTag") } >= 0
        existingConstraints.findIndexOf { it.contains(":__LiquibaseContext") } >= 0
        existingConstraints.findIndexOf { it.contains(":__LiquibaseLabel") } >= 0
        !enterpriseEdition() || existingConstraints.findIndexOf { it.contains(":__LiquibaseChangeSet") } >= 0
        def row = queryRunner.getSingleRow("""
            MATCH (n) RETURN LABELS(n) AS labels, n.dateCreated AS dateCreated, n.dateUpdated as dateUpdated
        """)
        row["labels"] == ["__LiquibaseChangeLog"]
        date(row["dateCreated"] as ZonedDateTime) > nowMinus(1, MINUTES)
        row["dateUpdated"] == null
    }

    def "ensures tag node uniqueness after initialization"() {
        given:
        historyService.init()

        when:
        2.times {
            queryRunner.run("CREATE (:__LiquibaseTag { tag: 'Guten Tag!' })")
        }

        then:
        def exception = thrown(ClientException)
        exception.message.contains("already exists with label `__LiquibaseTag` and property `tag` = 'Guten Tag!'")
    }

    def "ensures context node uniqueness after initialization"() {
        given:
        historyService.init()

        when:
        2.times {
            queryRunner.run("CREATE (:__LiquibaseContext { context: 'Chilly Con-text' })")
        }

        then:
        def exception = thrown(ClientException)
        exception.message.contains("already exists with label `__LiquibaseContext` and property `context` = 'Chilly Con-text'")
    }

    def "ensures label node uniqueness after initialization"() {
        given:
        historyService.init()

        when:
        2.times {
            queryRunner.run("CREATE (:__LiquibaseLabel { label: 'Label bleue' })")
        }

        then:
        def exception = thrown(ClientException)
        exception.message.contains("already exists with label `__LiquibaseLabel` and property `label` = 'Label bleue'")
    }

    @Requires({ enterpriseEdition() })
    def "ensures change set node uniqueness after initialization"() {
        given:
        historyService.init()

        when:
        2.times {
            queryRunner.run("CREATE (:__LiquibaseChangeSet { id: 'some ID', author: 'Jane Doe', changeLog: 'some/change/log' })")
        }

        then:
        def exception = thrown(ClientException)
        exception.message.contains("already exists with label `__LiquibaseChangeSet` and properties `id` = 'some ID', `author` = 'Jane Doe', `changeLog` = 'some/change/log'")
    }

    def "updates changelog, does not fail trying to recreate indices upon multiple initializations"() {
        when:
        2.times {
            historyService.init()
        }

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (n) RETURN LABELS(n) AS labels, n.dateCreated AS dateCreated, n.dateUpdated as dateUpdated
        """)
        def creationDate = date(row["dateCreated"] as ZonedDateTime)
        row["labels"] == ["__LiquibaseChangeLog"]
        creationDate > nowMinus(1, MINUTES)
        date(row["dateUpdated"] as ZonedDateTime) > creationDate
    }

    def "resets state"() {
        given:
        someInitialState()

        when:
        historyService.reset()

        then:
        stateIsReset()
    }

    def "destroys state, constraints, and data"() {
        given:
        queryRunner.createUniqueConstraint(TAG_CONSTRAINT_NAME, "__LiquibaseTag", "tag")
        queryRunner.createUniqueConstraint(CONTEXT_CONSTRAINT_NAME, "__LiquibaseContext", "context")
        queryRunner.createUniqueConstraint(LABEL_CONSTRAINT_NAME, "__LiquibaseLabel", "label")
        if (enterpriseEdition()) {
            queryRunner.createNodeKeyConstraint(CHANGE_SET_CONSTRAINT_NAME, "__LiquibaseChangeSet", "id", "author", "changeLog")
        }
        setField("ranChangeSets", historyService, singletonList(ranChangeSet("some ID", "some author", computeCheckSum("MATCH (n) RETURN n"), date(1986, 3, 4))))
        historyService.generateDeploymentId()
        manuallyCreateOrderedChangesets(ranChangeSet("older", "some author", computeCheckSum("CREATE (n:SomeNode)"), date(2019, 12, 25)),
                ranChangeSet("newer", "some author", computeCheckSum("MATCH (n:SomeNode) SET n:SomeExtraLabel"), date(2020, 12, 25)))
        manuallyAssignTag("guten-tag", "older")
        manuallyUpsertDisconnectedContext("discon-text")
        manuallyUpsertDisconnectedLabel("lone-label")
        manuallyAssignContext("context-1", "newer")
        manuallyAssignLabel("some-label", "older")


        when:
        historyService.destroy()

        then:
        getField("ranChangeSets", historyService) == null
        historyService.deploymentId == null
        queryRunner.listExistingConstraints().isEmpty()
        queryRunner.getSingleRow("MATCH (n) RETURN COUNT(n) AS count")["count"] == 0L
    }

    def "does not fail upon destroy call before database and service are initialized"() {
        when:
        historyService.destroy()

        then:
        getField("ranChangeSets", historyService) == null
        queryRunner.listExistingConstraints().isEmpty()
    }

    def "reads ran change sets from database upon first call"() {
        given:
        def checkSum = computeCheckSum("MATCH (n) SET n:SomeLabel\nMATCH (n) SET n:SomeLabel2")
        manuallyCreateOrderedChangesets(ranChangeSet("some ID", "some author", checkSum, date(1986, 3, 4), "some/logical/changeLog", "some/physical/changeLog"))

        when:
        def result = historyService.getRanChangeSets()

        then:
        result.size() == 1
        verifyAll(result.iterator().next()) {
            changeLog == "some/logical/changeLog"
            id == "some ID"
            author == "some author"
            lastCheckSum == checkSum
            dateExecuted == date(1986, 3, 4)
            tag == null
            execType == EXECUTED
            description == "some description"
            comments == "some comments"
            contextExpression.getContexts().isEmpty()
            labels.getLabels().isEmpty()
            deploymentId == "some deployment ID"
            storedChangeLog == "some/physical/changeLog"
        }
    }

    def "sorts ran change sets by ascending execution date first"() {
        given:
        def checkSum = computeCheckSum("MATCH (n) SET n:SomeLabel\nMATCH (n) SET n:SomeLabel2")
        manuallyCreateOrderedChangesets(ranChangeSet("older", "some author", checkSum, date(1986, 3, 4)),
                ranChangeSet("newer", "some author", checkSum, date(2020, 3, 4)),)

        when:
        def result = historyService.getRanChangeSets()

        then:
        result.collect { it.id } == ["older", "newer"]
    }

    def "sorts ran change sets by ascending order second (when dates are equal)"() {
        given:
        def checkSum = computeCheckSum("MATCH (n) SET n:SomeLabel\nMATCH (n) SET n:SomeLabel2")
        manuallyCreateOrderedChangesets(ranChangeSet("ordered first", "some author", checkSum, date(1986, 3, 4)),
                ranChangeSet("ordered second", "some author", checkSum, date(1986, 3, 4)),)

        when:
        def result = historyService.getRanChangeSets()

        then:
        result.collect { it.id } == ["ordered first", "ordered second"]
    }

    def "reads tagged ran change sets from database"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("some ID", "some author", computeCheckSum("MATCH (n) SET n:SomeLabel"), date(1986, 3, 4)))
        manuallyUpsertDisconnectedTag("merhaba")
        manuallyAssignTag("guten-tag", "some ID")

        when:
        def result = historyService.getRanChangeSets()

        then:
        result.size() == 1
        verifyAll(result.iterator().next()) {
            tag == "guten-tag"
        }
    }

    def "reads labeled ran change sets from database"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("some ID", "some author", computeCheckSum("MATCH (n) SET n:SomeLabel"), date(1986, 3, 4)))
        manuallyAssignLabel("label-bleue", "some ID")
        manuallyAssignLabel("label-rouge", "some ID")
        manuallyUpsertDisconnectedLabel("label-verte")

        when:
        def result = historyService.getRanChangeSets()

        then:
        result.size() == 1
        verifyAll(result.iterator().next()) {
            labels.getLabels() == new HashSet(Arrays.asList("label-bleue", "label-rouge"))
        }
    }

    def "reads ran change sets with contexts from database"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("some ID", "some author", computeCheckSum("MATCH (n) SET n:SomeLabel"), date(1986, 3, 4)))
        manuallyAssignContext("chili con text", "some ID")
        manuallyAssignContext("need context", "some ID")
        manuallyUpsertDisconnectedContext("lone context")

        when:
        def result = historyService.getRanChangeSets()

        then:
        result.size() == 1
        verifyAll(result.iterator().next()) {
            contextExpression.getContexts() == new HashSet(Arrays.asList("chili con text", "need context"))
        }
    }

    def "reads ran change sets from set field"() {
        given:
        def ranChangeSets = singletonList(ranChangeSet("some ID", "some author", computeCheckSum("MATCH (n) RETURN n"), date(1986, 3, 4)))
        setField("ranChangeSets", historyService, ranChangeSets)

        when:
        def results = historyService.getRanChangeSets()

        then:
        results == ranChangeSets
    }

    def "creates tag on single change set"() {
        given:
        def checkSum = computeCheckSum("MATCH (n) SET n:SomeLabel")
        manuallyCreateOrderedChangesets(ranChangeSet("some ID", "some author", checkSum, date(2019, 12, 25)),)

        when:
        historyService.tag("guten-tag")

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (tag:__LiquibaseTag {tag: 'guten-tag'})-[:TAGS]->(changeSet:__LiquibaseChangeSet {id: "some ID"}),
                  (changeSet)-[:IN_CHANGELOG]->(changeLog:__LiquibaseChangeLog)
            RETURN changeSet {.id, .author, .checkSum, tagContents: tag.tag, tagDateCreated: tag.dateCreated, tagDateUpdated: tag.dateUpdated, changeLogDateUpdated: changeLog.dateUpdated}
        """)["changeSet"] as Map<String, Object>
        containsAll(row, [id         : "some ID",
                          author     : "some author",
                          checkSum   : checkSum.toString(),
                          tagContents: "guten-tag"])
        date(row["tagDateCreated"] as ZonedDateTime) > nowMinus(1, MINUTES)
        row["tagDateUpdated"] == null
        date(row["changeLogDateUpdated"] as ZonedDateTime) > nowMinus(1, MINUTES)
    }

    def "updates existing tag on single change set"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("some ID", "some author", computeCheckSum("CREATE (n:SomeNode)"), date(2019, 12, 25)))
        manuallyAssignTag("guten-tag", "some ID", 1984, 6, 5)

        when:
        historyService.tag("guten-tag")

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (tag:__LiquibaseTag {tag: 'guten-tag'})-[:TAGS]->(:__LiquibaseChangeSet {id: "some ID"})
            RETURN tag {.tag, .dateCreated, .dateUpdated}
        """)["tag"] as Map<String, Object>
        containsAll(row, [tag: "guten-tag"])
        date(row["dateCreated"] as ZonedDateTime) == date(1984, 6, 5)
        date(row["dateUpdated"] as ZonedDateTime) > nowMinus(1, MINUTES)
    }

    def "creates tag on most recent change set (based on date first)"() {
        given:
        def checkSum1 = computeCheckSum("MATCH (n) SET n:SomeLabel1")
        def checkSum2 = computeCheckSum("MATCH (n) SET n:SomeLabel2")
        manuallyCreateOrderedChangesets(ranChangeSet("some ID 1", "some author", checkSum1, date(2020, 12, 25)),
                ranChangeSet("some ID 2", "some author", checkSum2, date(2019, 12, 25)))

        when:
        historyService.tag("guten-tag")

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (tag:__LiquibaseTag {tag: 'guten-tag'})-[:TAGS]->(changeSet:__LiquibaseChangeSet)-[:IN_CHANGELOG]->(:__LiquibaseChangeLog)
            RETURN changeSet {.id, .author, .checkSum, tagContents: tag.tag, tagCreatedDate: tag.dateCreated, tagUpdatedDate: tag.dateUpdated}
        """)["changeSet"] as Map<String, Object>
        containsAll(row, [id         : "some ID 1",
                          author     : "some author",
                          checkSum   : checkSum1.toString(),
                          tagContents: "guten-tag"])
        date(row["tagCreatedDate"] as ZonedDateTime) > nowMinus(1, MINUTES)
        row["tagUpdatedDate"] == null
    }

    def "creates tag on most recent change set (based on secondary order since dates are equal)"() {
        given:
        def sameDate = date(2020, 12, 25)
        def checkSum3 = computeCheckSum("MATCH (n) SET n:SomeLabel3")
        manuallyCreateOrderedChangesets( // third change set wins, as the order is higher
                ranChangeSet("some ID 1", "some author", computeCheckSum("MATCH (n) SET n:SomeLabel1"), sameDate),
                ranChangeSet("some ID 2", "some author", computeCheckSum("MATCH (n) SET n:SomeLabel2"), sameDate),
                ranChangeSet("some ID 3", "some author", checkSum3, sameDate),)

        when:
        historyService.tag("guten-tag")

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (tag:__LiquibaseTag {tag: 'guten-tag'})-[:TAGS]->(changeSet:__LiquibaseChangeSet)-[:IN_CHANGELOG]->(:__LiquibaseChangeLog)
            RETURN changeSet {.id, .author, .checkSum, tagContents: tag.tag, tagCreatedDate: tag.dateCreated, tagUpdatedDate: tag.dateUpdated}
        """)["changeSet"] as Map<String, Object>
        containsAll(row, [id         : "some ID 3",
                          author     : "some author",
                          checkSum   : checkSum3.toString(),
                          tagContents: "guten-tag"])
    }

    def "updates in-memory change sets with new tag"() {
        given:
        def changeSet1 = ranChangeSet("some ID 1", "some author", computeCheckSum("MATCH (n) SET n:SomeLabel1"), date(2020, 12, 25))
        def changeSet2 = ranChangeSet("some ID 2", "some author", computeCheckSum("MATCH (n) SET n:SomeLabel1"), date(2019, 12, 25))
        setField("ranChangeSets", historyService, [changeSet1, changeSet2])
        manuallyCreateOrderedChangesets(changeSet1, changeSet2)

        expect:
        changeSet1.getTag() == null
        changeSet2.getTag() == null

        when:
        historyService.tag("guten-tag")

        then:
        changeSet1.getTag() == "guten-tag"
        changeSet2.getTag() == null
    }

    def "creates disconnected tag in empty database"() {
        when:
        historyService.tag("guten-tag")

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (tag:__LiquibaseTag {tag: 'guten-tag'})
            OPTIONAL MATCH (tag)-[r]->()
            RETURN tag {.tag, .dateCreated, .dateUpdated, relationCount: SIZE(COLLECT(r))}
        """)["tag"] as Map<String, Object>
        containsAll(row, [tag: "guten-tag", relationCount: 0L])
        date(row["dateCreated"] as ZonedDateTime) > nowMinus(1, MINUTES)
        row["dateUpdated"] == null
    }

    def "creates another disconnected tag in empty database"() {
        given:
        manuallyUpsertDisconnectedTag("guten-tag", 2020, 12, 25)

        when:
        historyService.tag("guten-tag-2")

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (tag:__LiquibaseTag)
            WITH tag
            ORDER BY tag.tag ASC
            RETURN COLLECT(tag.tag) AS tags
        """)
        row["tags"] == ["guten-tag", "guten-tag-2"]
    }

    def "updates disconnected tag in empty database"() {
        given:
        manuallyUpsertDisconnectedTag("guten-tag", 2020, 12, 25)

        when:
        historyService.tag("guten-tag")

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (tag:__LiquibaseTag {tag: 'guten-tag'})
            OPTIONAL MATCH (tag)-[r]->()
            RETURN tag {.tag, .dateCreated, .dateUpdated, relationCount: SIZE(COLLECT(r))}
        """)["tag"] as Map<String, Object>
        containsAll(row, [tag: "guten-tag", relationCount: 0L])
        date(row["dateCreated"] as ZonedDateTime) == date(2020, 12, 25)
        date(row["dateUpdated"] as ZonedDateTime) > nowMinus(1, MINUTES)
    }

    def "links existing tag to the newest change set"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("older", "some author", computeCheckSum("CREATE (n:SomeNode)"), date(2019, 12, 25)),
                ranChangeSet("newer", "some author", computeCheckSum("MATCH (n:SomeNode) SET n:SomeExtraLabel"), date(2020, 12, 25)))
        manuallyAssignTag("guten-tag", "older", 2019, 12, 25)

        when:
        historyService.tag("guten-tag")

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (tag:__LiquibaseTag)-[:TAGS]->(changeSet:__LiquibaseChangeSet)
            RETURN tag.tag AS tag, changeSet.id AS changeSetId
        """)
        row["tag"] == "guten-tag"
        row["changeSetId"] == "newer"
    }

    def "links disconnected tag to the newest change set"() {
        given:
        manuallyUpsertDisconnectedTag("guten-tag", 2015, 12, 25)
        manuallyCreateOrderedChangesets(ranChangeSet("older", "some author", computeCheckSum("CREATE (n:SomeNode)"), date(2019, 12, 25)),
                ranChangeSet("newer", "some author", computeCheckSum("MATCH (n:SomeNode) SET n:SomeExtraLabel"), date(2020, 12, 25)))

        when:
        historyService.tag("guten-tag")

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (tag:__LiquibaseTag)-[:TAGS]->(changeSet:__LiquibaseChangeSet)
            RETURN tag.tag AS tag, changeSet.id AS changeSetId
        """)
        row["tag"] == "guten-tag"
        row["changeSetId"] == "newer"
    }

    def "allows multiple tags per change set"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("older", "some author", computeCheckSum("CREATE (n:SomeNode)"), date(2019, 12, 25)),
                ranChangeSet("newer", "some author", computeCheckSum("MATCH (n:SomeNode) SET n:SomeExtraLabel"), date(2020, 12, 25)))
        manuallyAssignTag("gluten-day", "newer", 2019, 12, 25)
        manuallyAssignTag("glutton-bay", "older", 2015, 12, 25)

        when:
        historyService.tag("guten-tag")

        then:
        def rows = queryRunner.getRows("""
            MATCH (tag:__LiquibaseTag)-[:TAGS]->(changeSet:__LiquibaseChangeSet)
            RETURN tag.tag AS tag, changeSet.id AS changeSetId
            ORDER BY tag.tag ASC
        """)
        rows.size() == 3
        def iterator = rows.iterator()
        def row1 = iterator.next()
        row1["tag"] == "gluten-day"
        row1["changeSetId"] == "newer"
        def row2 = iterator.next()
        row2["tag"] == "glutton-bay"
        row2["changeSetId"] == "older"
        def row3 = iterator.next()
        row3["tag"] == "guten-tag"
        row3["changeSetId"] == "newer"
    }

    def "tags a new change set without affecting previous change sets tagged with another tag value"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("older", "some author", computeCheckSum("CREATE (n:SomeNode)"), date(2019, 12, 25)),
                ranChangeSet("newer", "some author", computeCheckSum("MATCH (n:SomeNode) SET n:SomeExtraLabel"), date(2020, 12, 25)))
        manuallyAssignTag("gluten-free", "older", 2019, 12, 25)

        when:
        historyService.tag("guten-tag")

        then:
        def rows = queryRunner.getRows("""
            MATCH (tag:__LiquibaseTag)-[:TAGS]->(changeSet:__LiquibaseChangeSet)
            RETURN tag.tag AS tag, changeSet.id AS changeSetId
            ORDER BY tag ASC
        """)
        rows.size() == 2
        def iterator = rows.iterator()
        def row1 = iterator.next()
        row1["tag"] == "gluten-free"
        row1["changeSetId"] == "older"
        def row2 = iterator.next()
        row2["tag"] == "guten-tag"
        row2["changeSetId"] == "newer"
    }

    def "finds disconnected tags"() {
        when:
        manuallyUpsertDisconnectedTag("guten-tag", 2020, 12, 25)

        then:
        historyService.tagExists("guten-tag")
        !historyService.tagExists("göruşürüz")
    }

    def "finds connected tags"() {
        when:
        manuallyCreateOrderedChangesets(ranChangeSet("change-set", "some author", computeCheckSum("CREATE (n:SomeNode)"), date(2019, 12, 25)),)
        manuallyAssignTag("guten-tag", "change-set", 2019, 12, 25)

        then:
        historyService.tagExists("guten-tag")
        !historyService.tagExists("göruşürüz")
    }

    def "finds any tags"() {
        when:
        manuallyUpsertDisconnectedTag("bonjour", 2018, 12, 25)
        manuallyCreateOrderedChangesets(ranChangeSet("change-set", "some author", computeCheckSum("CREATE (n:SomeNode)"), date(2019, 12, 25)),)
        manuallyAssignTag("guten-tag", "change-set", 2019, 12, 25)

        then:
        historyService.tagExists("bonjour")
        historyService.tagExists("guten-tag")
    }

    def "finds ran change set"() {
        given:
        def ranChangeSet = ranChangeSet("some ID", "some author", computeCheckSum("MATCH (n) RETURN n"), date(1986, 3, 4), "some/changeLog/path")
        setField("ranChangeSets", historyService, singletonList(ranChangeSet))

        when:
        def foundRanChangeSet = historyService.getRanChangeSet(changeSet("some ID", "some author", "some/changeLog/path"))
        def notFoundRanChangeSet1 = historyService.getRanChangeSet(changeSet("some other ID", "some author", "some/changeLog/path"))
        def notFoundRanChangeSet2 = historyService.getRanChangeSet(changeSet("some ID", "some other author", "some/changeLog/path"))
        def notFoundRanChangeSet3 = historyService.getRanChangeSet(changeSet("some ID", "some author", "some/other/changeLog/path"))

        then:
        foundRanChangeSet == ranChangeSet
        notFoundRanChangeSet1 == null
        notFoundRanChangeSet2 == null
        notFoundRanChangeSet3 == null
    }

    def "finds ran change set date"() {
        given:
        def ranDate = date(1986, 3, 4)
        def ranChangeSet = ranChangeSet("some ID", "some author", computeCheckSum("MATCH (n) RETURN n"), ranDate, "some/changeLog/path")
        setField("ranChangeSets", historyService, singletonList(ranChangeSet))

        when:
        def foundRanDate = historyService.getRanDate(changeSet("some ID", "some author", "some/changeLog/path"))
        def notFoundRanDate1 = historyService.getRanDate(changeSet("some other ID", "some author", "some/changeLog/path"))
        def notFoundRanDate2 = historyService.getRanDate(changeSet("some ID", "some other author", "some/changeLog/path"))
        def notFoundRanDate3 = historyService.getRanDate(changeSet("some ID", "some author", "some/other/changeLog/path"))

        then:
        foundRanDate == ranDate
        notFoundRanDate1 == null
        notFoundRanDate2 == null
        notFoundRanDate3 == null
    }

    def "generates deployment ID upon first call"() {
        when:
        historyService.generateDeploymentId()

        then:
        historyService.deploymentId != ""
    }

    def "does not regenerate deployment ID on subsequent calls"() {
        given:
        def deploymentIds = new HashSet()

        when:
        2.times {
            historyService.generateDeploymentId()
            deploymentIds << historyService.deploymentId
        }

        then:
        deploymentIds.size() == 1
    }

    def "resets deployment ID"() {
        given:
        historyService.generateDeploymentId()

        when:
        historyService.resetDeploymentId()

        then:
        historyService.deploymentId == null
    }

    def "replaces check sum of matching change set"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("change-set", "some author", computeCheckSum("CREATE (n:SomeNode)"), date(2019, 12, 25), "some/changeSet/path"),)
        def updatedChangeSet = changeSet("change-set", "some author", "some/changeSet/path", change("MATCH (n) DETACH DELETE n"))
        def nonMatchingChangeSet = changeSet("change-set-2", "some author", "some/changeSet/path", change("MATCH (n) SET n:SomeLabel"))

        when:
        historyService.replaceChecksum(updatedChangeSet)

        then:
        def row = queryRunner.getSingleRow("MATCH (changeSet:__LiquibaseChangeSet)-[:IN_CHANGELOG]->(changeLog:__LiquibaseChangeLog) RETURN changeSet {.checkSum, changeLogDateUpdated: changeLog.dateUpdated}")["changeSet"]
        date(row["changeLogDateUpdated"] as ZonedDateTime) > nowMinus(1, MINUTES)
        def checkSum = (String) row["checkSum"]
        CheckSum.parse(checkSum) == updatedChangeSet.generateCheckSum()
        CheckSum.parse(checkSum) != nonMatchingChangeSet.generateCheckSum()
    }

    def "resets state once check sum is replaced"() {
        given:
        def ranChangeSet = ranChangeSet("change-set", "some author", computeCheckSum("CREATE (n:SomeNode)"), date(2019, 12, 25), "some/changeSet/path")
        someInitialState(ranChangeSet)
        manuallyCreateOrderedChangesets(ranChangeSet)
        def updatedChangeSet = changeSet("change-set", "some author", "some/changeSet/path", change("MATCH (n) DETACH DELETE n"))

        when:
        historyService.replaceChecksum(updatedChangeSet)

        then:
        stateIsReset()
    }

    def "computes run statuses of change sets"() {
        given:
        def storedChangeSet = ranChangeSet("some ID", "some author", computeCheckSum("MATCH (n) RETURN n"), date(1986, 3, 4), "some/changeLog/path")
        def storedChangeSetWithNullCheckSum = ranChangeSet("other ID with no checksum", "some author", null, date(1986, 3, 4), "some/changeLog/path")
        manuallyCreateOrderedChangesets(storedChangeSet, storedChangeSetWithNullCheckSum)
        setField("ranChangeSets", historyService, [storedChangeSet, storedChangeSetWithNullCheckSum])

        when:
        def nonMatchingChangeSet = changeSet("some non-matching ID", "some author", "some/changeLog/path")
        def matchingNullCheckSumChangeSet = changeSet("other ID with no checksum", "some author", "some/changeLog/path")
        def matchingChangeSet = changeSet("some ID", "some author", "some/changeLog/path", change("MATCH (n) RETURN n"))
        def matchingRunOnChangeChangeSetWithDifferentCheckSum = changeSet("some ID", "some author", "some/changeLog/path", true, change("MATCH (n) DETACH DELETE n"))
        def matchingImmutableChangeSetWithDifferentCheckSum = changeSet("some ID", "some author", "some/changeLog/path", change("MATCH (n) DETACH DELETE n"))

        then:
        historyService.getRunStatus(nonMatchingChangeSet) == NOT_RAN
        historyService.getRunStatus(matchingNullCheckSumChangeSet) == ALREADY_RAN
        queryRunner.getSingleRow( // ran change sets with null check sums are always updated
                """MATCH (s:__LiquibaseChangeSet {id: "${storedChangeSetWithNullCheckSum.id}"}) RETURN s.checkSum AS checkSum""")["checkSum"] == matchingNullCheckSumChangeSet.generateCheckSum().toString()
        historyService.getRunStatus(matchingChangeSet) == ALREADY_RAN
        historyService.getRunStatus(matchingRunOnChangeChangeSetWithDifferentCheckSum) == RUN_AGAIN
        historyService.getRunStatus(matchingImmutableChangeSetWithDifferentCheckSum) == INVALID_MD5SUM
    }

    def "computes next sequence value"() {
        given:
        def changeSets = [ranChangeSet("some ID", "some author", computeCheckSum("MATCH (n) RETURN n"), date(1986, 3, 4)),
                          ranChangeSet("other ID with no checksum", "some author", null, date(1986, 3, 4))]
        manuallyCreateOrderedChangesets(*changeSets)
        queryRunner.run("""MATCH (c:__LiquibaseChangeSet {id: "some ID"}) DETACH DELETE c""") // create sequence gaps

        when:
        def value = historyService.getNextSequenceValue()

        then:
        value == changeSets.size()
    }

    def "computes next sequence value from field, if set"() {
        given:
        setField("lastChangeSetSequenceValue", historyService, 41)

        when:
        def value = historyService.getNextSequenceValue()

        then:
        value == 42
    }

    def "clears all checksums"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("other ID with no checksum", "some author", null, date(1986, 3, 4)),
                ranChangeSet("some ID", "some author", computeCheckSum("MATCH (n) RETURN n"), date(1986, 3, 4)))

        when:
        historyService.clearAllCheckSums()

        then:
        listCheckSums() == ["", ""]
        def dateUpdated = queryRunner.getSingleRow("MATCH (changeLog:__LiquibaseChangeLog) RETURN changeLog.dateUpdated AS dateUpdated")["dateUpdated"]
        date(dateUpdated as ZonedDateTime) > nowMinus(1, MINUTES)
    }

    def "upgrades null checksums in context, resets state"() {
        given:
        def unchangedCheckSum = computeCheckSum("MATCH (n) RETURN n")
        manuallyCreateOrderedChangesets(ranChangeSet("some ID 1", "some author", null, date(1986, 3, 4), "some/path"),
                ranChangeSet("some ID 2", "some author", unchangedCheckSum, date(1986, 3, 4), "some/path"),
                ranChangeSet("some ID 3", "some author", null, date(1986, 3, 4), "some/path"))
        manuallyAssignContext("context1", "some ID 1")
        manuallyAssignContext("context1", "some ID 2")
        manuallyAssignContext("context2", "some ID 3")

        def changeLog = new DatabaseChangeLog().with {
            it.addChangeSet(changeSet("some ID 1", "some author", "some/path", false, "context1"))
            it.addChangeSet(changeSet("some ID 2", "some author", "some/path", false, "context1", new RawSQLChange("MATCH (n) RETURN n")))
            it.addChangeSet(changeSet("some ID 3", "some author", "some/path", false, "context2"))
            return it
        }

        when:
        historyService.upgradeChecksums(changeLog, new Contexts("context1"), new LabelExpression())

        then:
        stateIsReset()
        def checkSums = listCheckSums()
        checkSums.size() == 3
        checkSums[0] != "" && CheckSum.parse(checkSums[0]) != null
        CheckSum.parse(checkSums[1]) == unchangedCheckSum
        checkSums[2] == ""
    }

    def "removes change set from history"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("some ID 1", "some author", computeCheckSum("MATCH (n) SET n:FooBar"), date(1986, 3, 4), "some/path"),
                ranChangeSet("some ID 2", "some author", computeCheckSum("MATCH (n) SET n:FooFighters"), date(1986, 3, 4), "some/path"))

        when:
        historyService.removeFromHistory(changeSet("some ID 1", "some author", "some/path"))

        then:
        getField("ranChangeSets", historyService) == null
        def row = queryRunner.getSingleRow("MATCH (changeSet:__LiquibaseChangeSet)-[:IN_CHANGELOG]->(changeLog:__LiquibaseChangeLog) RETURN changeLog {.dateUpdated, changeSetIds: COLLECT(changeSet.id)}")["changeLog"]
        date(row["dateUpdated"] as ZonedDateTime) > nowMinus(1, MINUTES)
        row["changeSetIds"] == ["some ID 2"]
    }

    def "removes change set from memory as well"() {
        given:
        def ranChangeSets = [ranChangeSet("some ID 1", "some author", computeCheckSum("MATCH (n) SET n:FooBar"), date(1986, 3, 4), "some/path"),
                             ranChangeSet("some ID 2", "some author", computeCheckSum("MATCH (n) SET n:FooFighters"), date(1986, 3, 4), "some/path")]
        setField("ranChangeSets", historyService, ranChangeSets.clone())
        manuallyCreateOrderedChangesets(*ranChangeSets)

        when:
        historyService.removeFromHistory(changeSet("some ID 1", "some author", "some/path"))

        then:
        def changeSets = getField("ranChangeSets", historyService)
        changeSets == [ranChangeSets[1]]
    }

    def "does not save change set when execution failed or skipped"() {
        when:
        historyService.setExecType(changeSet("some-id", "some-author", "some/path"), FAILED)
        historyService.setExecType(changeSet("some-id", "some-author", "some/path"), SKIPPED)

        then:
        queryRunner.getSingleRow("MATCH (c:__LiquibaseChangeSet) RETURN COUNT(c) AS count")["count"] == 0L
    }

    def "updates change set when re-ran, including execution order"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("some-id-1", "some-author", CheckSum.compute("MATCH (n) SET n.foo = true"), date(2020, 1, 4), "some/path"),
                ranChangeSet("some-id-2", "some-author", CheckSum.compute("MATCH (n) SET n.foo = false"), date(2020, 1, 4)))
        def updatedChangeSet = changeSet("some-id-1", "some-author", "some/path", true, null, new RawSQLChange("MATCH (n) DETACH DELETE n"))

        when:
        historyService.setExecType(updatedChangeSet, RERAN)

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (c:__LiquibaseChangeSet {id: "some-id-1"})-[exec:IN_CHANGELOG]->(l:__LiquibaseChangeLog)
            RETURN c {.id, .author, .checkSum, .execType, changeLogDateUpdated: l.dateUpdated, dateExecuted: exec.dateExecuted, orderExecuted: exec.orderExecuted} AS changeSet
        """)["changeSet"] as Map<String, Object>
        row["id"] == "some-id-1"
        row["author"] == "some-author"
        CheckSum.parse((String) row["checkSum"]) == updatedChangeSet.generateCheckSum()
        valueOf((String) row["execType"]) == RERAN
        date(row["changeLogDateUpdated"] as ZonedDateTime) > nowMinus(1, MINUTES)
        date(row["dateExecuted"] as ZonedDateTime) > nowMinus(1, MINUTES)
        row["orderExecuted"] == 2L
    }

    def "persists change set with execution type"() {
        given:
        queryRunner.run("CREATE (:__LiquibaseChangeLog)")
        historyService.generateDeploymentId()
        def newChangeSet = new ChangeSet("some-id",
                "some-author",
                false,
                false,
                "some/path",
                null,
                null,
                null,
                null)
        newChangeSet.setStoredFilePath("some/stored/path")
        newChangeSet.setComments("comments")
        newChangeSet.addChange(new RawSQLChange("MATCH (n) DETACH DELETE n"))

        when:
        historyService.setExecType(newChangeSet, MARK_RAN)

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (c:__LiquibaseChangeSet)-[exec:IN_CHANGELOG]->(l:__LiquibaseChangeLog)
            RETURN c {
                .changeLog, 
                .id, 
                .author, 
                .checkSum, 
                .execType, 
                .description, 
                .comments, 
                .deploymentId, 
                .storedChangeLog, 
                .liquibaseVersion, 
                changeLogDateUpdated: l.dateUpdated, 
                dateExecuted: exec.dateExecuted, 
                orderExecuted: exec.orderExecuted} AS changeSet
        """)["changeSet"] as Map<String, Object>
        row["changeLog"] == "some/path"
        row["id"] == "some-id"
        row["author"] == "some-author"
        CheckSum.parse((String) row["checkSum"]) == newChangeSet.generateCheckSum()
        valueOf((String) row["execType"]) == MARK_RAN
        row["description"] != ""
        row["comments"] == "comments"
        row["deploymentId"] != ""
        row["storedChangeLog"] == "some/stored/path"
        row["liquibaseVersion"] != ""
        date(row["changeLogDateUpdated"] as ZonedDateTime) > nowMinus(1, MINUTES)
        date(row["dateExecuted"] as ZonedDateTime) > nowMinus(1, MINUTES)
        row["orderExecuted"] == 1L
    }

    def "persists change set with contexts"() {
        given:
        queryRunner.run("CREATE (:__LiquibaseChangeLog)")
        def newChangeSet = new ChangeSet("some-id",
                "some-author",
                false,
                false,
                "some/path",
                "context1,context2",
                null,
                null,
                null)

        when:
        historyService.setExecType(newChangeSet, EXECUTED)

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (changeSet:__LiquibaseChangeSet)<-[:CONTEXTUALIZES]-(context:__LiquibaseContext)
            RETURN changeSet {
                .id, 
                contexts: COLLECT(context.context)
            }""")["changeSet"] as Map<String, Object>
        row["id"] == "some-id"
        row["contexts"] as Set == ["context1", "context2"] as Set
    }

    def "updates change set when re-ran, resets contexts"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("some-id-1", "some-author", CheckSum.compute("MATCH (n) SET n.foo = true"), date(2020, 1, 4), "some/path"),
                ranChangeSet("some-id-2", "some-author", CheckSum.compute("MATCH (n) SET n.foo = false"), date(2020, 1, 4)))
        manuallyAssignContext("initial-context-1", "some-id-1")
        manuallyAssignContext("initial-context-2", "some-id-1")

        def updatedChangeSet = new ChangeSet("some-id-1",
                "some-author",
                false,
                false,
                "some/path",
                "new-context-1,new-context-2",
                null,
                null,
                null)

        when:
        historyService.setExecType(updatedChangeSet, RERAN)

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (changeSet:__LiquibaseChangeSet)<-[:CONTEXTUALIZES]-(context:__LiquibaseContext)
            RETURN changeSet {
                .id, 
                contexts: COLLECT(context.context)
            }""")["changeSet"] as Map<String, Object>
        row["id"] == "some-id-1"
        row["contexts"] as Set == ["new-context-1", "new-context-2"] as Set
    }

    def "persists change set with labels"() {
        given:
        queryRunner.run("CREATE (:__LiquibaseChangeLog)")
        def newChangeSet = new ChangeSet("some-id",
                "some-author",
                false,
                false,
                "some/path",
                null,
                null,
                null,
                null)
        newChangeSet.setLabels(new Labels("label1", "label2"))

        when:
        historyService.setExecType(newChangeSet, EXECUTED)

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (changeSet:__LiquibaseChangeSet)<-[:LABELS]-(label:__LiquibaseLabel)
            RETURN changeSet {
                .id, 
                labels: COLLECT(label.label)
            }""")["changeSet"] as Map<String, Object>
        row["id"] == "some-id"
        row["labels"] as Set == ["label1", "label2"] as Set
    }

    def "updates change set when re-ran, resets labels"() {
        given:
        manuallyCreateOrderedChangesets(ranChangeSet("some-id-1", "some-author", CheckSum.compute("MATCH (n) SET n.foo = true"), date(2020, 1, 4), "some/path"),
                ranChangeSet("some-id-2", "some-author", CheckSum.compute("MATCH (n) SET n.foo = false"), date(2020, 1, 4)))
        manuallyAssignLabel("initial-label-1", "some-id-1")
        manuallyAssignLabel("initial-label-2", "some-id-1")

        def updatedChangeSet = new ChangeSet("some-id-1",
                "some-author",
                false,
                false,
                "some/path",
                null,
                null,
                null,
                null)
        updatedChangeSet.setLabels(new Labels("new-label-1", "new-label-2"))

        when:
        historyService.setExecType(updatedChangeSet, RERAN)

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (changeSet:__LiquibaseChangeSet)<-[:LABELS]-(label:__LiquibaseLabel)
            RETURN changeSet {
                .id, 
                labels: COLLECT(label.label)
            }""")["changeSet"] as Map<String, Object>
        row["id"] == "some-id-1"
        row["labels"] as Set == ["new-label-1", "new-label-2"] as Set
    }

    def "rejects change sets with several tag changes (XML schema only allows one)"() {
        given:
        def newChangeSet = new ChangeSet("some-id",
                "some-author",
                false,
                false,
                "some/path",
                null,
                null,
                null,
                null)
        newChangeSet.addChange(tagChange("tag-1"))
        newChangeSet.addChange(tagChange("tag-2"))

        when:
        historyService.setExecType(newChangeSet, EXECUTED)

        then:
        def exception = thrown(DatabaseException)
        exception.message == "Could not persist change set some/path::some-id::some-author with execution type EXECUTED"
        exception.cause.message == "A change set can only declare one tag, but found [tag-1, tag-2]"
    }

    def "updates change set when re-ran, resets tag connections to any change sets, and change set connections to any tag"() {
        given:
        manuallyUpsertDisconnectedTag("unrelated-tag")
        manuallyCreateOrderedChangesets(ranChangeSet("some-id-1", "some-author", CheckSum.compute("MATCH (n) SET n.foo = true"), date(2020, 1, 4)),
                ranChangeSet("some-id-2", "some-author", CheckSum.compute("MATCH (n) SET n.foo = false"), date(2020, 1, 4), "some/path"))
        manuallyAssignTag("tag", "some-id-1", 1986, 3, 4)
        def updatedChangeSet = new ChangeSet("some-id-2",
                "some-author",
                false,
                false,
                "some/path",
                null,
                null,
                null,
                null)
        updatedChangeSet.addChange(tagChange("tag"))

        when:
        historyService.setExecType(updatedChangeSet, RERAN)

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (changeSet:__LiquibaseChangeSet)<-[:TAGS]-(tag:__LiquibaseTag)
            RETURN changeSet {
                .id, 
                tags: COLLECT(tag {.tag, .dateCreated, .dateUpdated})
            }""")["changeSet"] as Map<String, Object>
        row["id"] == "some-id-2"
        def tags = row["tags"] as Set
        tags.size() == 1
        def tag = tags.iterator().next()
        tag["tag"] == "tag"
        tag["dateCreated"] == ZonedDateTime.ofInstant(date(1986, 3, 4).toInstant(), TIMEZONE)
        date(tag["dateUpdated"] as ZonedDateTime) > nowMinus(1, MINUTES)
    }

    private void someInitialState(RanChangeSet persistedChangeSet = ranChangeSet("some ID", "some author", computeCheckSum("MATCH (n) RETURN n"), date(1986, 3, 4))) {
        setField("ranChangeSets", historyService, singletonList(persistedChangeSet))
        historyService.generateDeploymentId()
        setField("lastChangeSetSequenceValue", historyService, 42)
    }

    private boolean stateIsReset() {
        return getField("ranChangeSets", historyService) == null && historyService.deploymentId == null && getField("lastChangeSetSequenceValue", historyService) == null
    }

    private manuallyCreateOrderedChangesets(RanChangeSet... changeSets) {
        def queries =
                ["CREATE (changeLog:__LiquibaseChangeLog)"] + (changeSets.toList()
                        .withIndex()
                        .collect { RanChangeSet changeSet, Integer index ->
                            def checkSumLine = changeSet.lastCheckSum == null ? "" : """
                                        |    checkSum: "${changeSet.lastCheckSum.toString()}","""
                            return """
                                        | CREATE (changeSet_$index:__LiquibaseChangeSet {
                                        |    changeLog: "${changeSet.changeLog}",
                                        |    storedChangeLog: "${changeSet.storedChangeLog}",
                                        |    id: "${changeSet.id}",
                                        |    author: "${changeSet.author}",
                                        |    execType: "${changeSet.execType.name()}",
                                        |    description: "${changeSet.description}",
                                        |    comments: "${changeSet.comments}",
                                        |    deploymentId: "${changeSet.deploymentId}",${checkSumLine}
                                        |    liquibaseVersion: "${changeSet.liquibaseVersion}"
                                        | }),
                                        | (changeLog)<-[:IN_CHANGELOG {
                                        |    dateExecuted: DATETIME({epochMillis:${changeSet.dateExecuted.time}}),
                                        |    orderExecuted: ${index}
                                        | }]-(changeSet_$index)
                                    """.stripMargin().trim()
                        })
        queryRunner.run(queries.join("\n"))
    }

    private manuallyAssignTag(String tag, String changeSetId) {
        manuallyAssignTag(tag, changeSetId, 1986, 3, 4)
    }


    private manuallyAssignTag(String tag, String changeSetId, int year, int month, int day) {
        manuallyUpsertDisconnectedTag(tag, year, month, day)
        queryRunner.run("""
            MATCH (changeSet:__LiquibaseChangeSet {id: "$changeSetId"})
            MATCH (tag:__LiquibaseTag {tag: "$tag"})
            CREATE (tag)-[:TAGS]->(changeSet)
        """)
    }

    private manuallyUpsertDisconnectedTag(String tag) {
        manuallyUpsertDisconnectedTag(tag, 2021, 1, 1)
    }

    private manuallyUpsertDisconnectedTag(String tag, int year, int month, int day) {
        queryRunner.run("""
            MERGE (t:__LiquibaseTag {tag: "$tag"}) ON CREATE SET t.dateCreated = DATETIME({year:$year, month:$month, day:$day})
        """)
    }

    private manuallyAssignLabel(String label, String changeSetId) {
        manuallyAssignLabel(label, changeSetId, 1984, 5, 6)
    }

    private manuallyAssignLabel(String label, String changeSetId, int year, int month, int day) {
        manuallyUpsertDisconnectedLabel(label, year, month, day)
        queryRunner.run("""
            MATCH (changeSet:__LiquibaseChangeSet {id: "$changeSetId"})
            MATCH (label:__LiquibaseLabel {label: "$label"})
            CREATE (label)-[:LABELS]->(changeSet)
        """)
    }

    private manuallyUpsertDisconnectedLabel(String label) {
        manuallyUpsertDisconnectedLabel(label, 2020, 12, 25)
    }

    private manuallyUpsertDisconnectedLabel(String label, int year, int month, int day) {
        queryRunner.run("""
            MERGE (l:__LiquibaseLabel {label: "$label"}) ON CREATE SET l.dateCreated = DATETIME({year:$year, month:$month, day:$day})
        """)
    }

    private manuallyAssignContext(String context, String changeSetId) {
        manuallyAssignContext(context, changeSetId, 1984, 5, 6)
    }

    private manuallyAssignContext(String context, String changeSetId, int year, int month, int day) {
        manuallyUpsertDisconnectedContext(context, year, month, day)
        queryRunner.run("""
            MATCH (changeSet:__LiquibaseChangeSet {id: "$changeSetId"})
            MATCH (context:__LiquibaseContext {context: "$context"})
            CREATE (context)-[:CONTEXTUALIZES]->(changeSet)
        """)
    }

    private manuallyUpsertDisconnectedContext(String context) {
        manuallyUpsertDisconnectedContext(context, 2020, 1, 1)
    }

    private manuallyUpsertDisconnectedContext(String context, int year, int month, int day) {
        queryRunner.run("""
            MERGE (c:__LiquibaseContext {context: "$context"}) ON CREATE SET c.dateCreated = DATETIME({year:$year, month:$month, day:$day})
        """)
    }

    private static RanChangeSet ranChangeSet(String id, String author, CheckSum checkSum = null, Date date, String logicalChangeLogPath = "some/logical/path", String physicalChangeLogPath = "some/physical/path") {
        def ranChangeSet = new RanChangeSet(logicalChangeLogPath,
                id,
                author,
                checkSum,
                date,
                null,
                EXECUTED,
                "some description",
                "some comments",
                null,
                null,
                "some deployment ID",
                physicalChangeLogPath)
        ranChangeSet.liquibaseVersion = "4.1.1"
        return ranChangeSet
    }

    private static ChangeSet changeSet(String id, String author, String changeLogPath, boolean runOnChange = false, String contexts = null, Change... changes) {
        def changeSet = new ChangeSet(id, author, false, runOnChange, changeLogPath, contexts, null, new DatabaseChangeLog(changeLogPath))
        changes.each { change -> changeSet.addChange(change)
        }
        return changeSet
    }

    private static Change change(String cypher) {
        return new RawSQLChange(cypher)
    }

    // inspired from liquibase.changelog.ChangeSet.generateCheckSum
    private static CheckSum computeCheckSum(String... queries) {
        def builder = new StringBuilder()
        queries.each { query -> builder.append(new RawSQLChange(query).generateCheckSum()).append(":") }
        return CheckSum.compute(builder.toString())
    }

    private List<String> listCheckSums() {
        def checkSums = (List<String>) queryRunner.getSingleRow("""
            MATCH (c:__LiquibaseChangeSet)-[exec:IN_CHANGELOG]->(:__LiquibaseChangeLog) 
            WITH c, exec 
            ORDER BY exec.dateExecuted ASC, exec.orderExecuted ASC
            RETURN COLLECT(COALESCE(c.checkSum, "")) AS checkSums
        """)["checkSums"]
        checkSums
    }

    private static TagDatabaseChange tagChange(String tag) {
        def result = new TagDatabaseChange()
        result.setTag(tag)
        return result
    }
}
