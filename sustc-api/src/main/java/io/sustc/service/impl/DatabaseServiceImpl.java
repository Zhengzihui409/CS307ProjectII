package io.sustc.service.impl;

import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {
    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //TODO: replace this with your own student IDs in your group
        return Arrays.asList(12412733,12312312);
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    public void importData(
            List<ReviewRecord> reviewRecords,
            List<UserRecord> userRecords,
            List<RecipeRecord> recipeRecords)  {
        // ddl to create tables.
        createTables();
        // TODO: implement your import logic
        String insertUserSQL = "INSERT INTO users (AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(insertUserSQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                UserRecord userRecord = userRecords.get(i);
                ps.setLong(1, userRecord.getAuthorId());
                ps.setString(2, userRecord.getAuthorName());
                ps.setString(3, userRecord.getGender());
                ps.setInt(4, userRecord.getAge());
                ps.setInt(5, userRecord.getFollowers());
                ps.setInt(6, userRecord.getFollowing());
                ps.setString(7, userRecord.getPassword());
                ps.setBoolean(8, userRecord.isDeleted());
            }
            @Override
            public int getBatchSize() {
                return userRecords.size();
            }
        });
        String insertRecipeSQL = "INSERT INTO recipes (RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, Description, RecipeCategory, AggregatedRating, ReviewCount, Calories, FatContent, SaturatedFatContent, CholesterolContent, SodiumContent, CarbohydrateContent, FiberContent, SugarContent, ProteinContent, RecipeServings, RecipeYield) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(insertRecipeSQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RecipeRecord recipeRecord = recipeRecords.get(i);
                ps.setLong(1, recipeRecord.getRecipeId());
                ps.setString(2, recipeRecord.getName());
                ps.setLong(3, recipeRecord.getAuthorId());
                ps.setString(4, recipeRecord.getCookTime());
                ps.setString(5, recipeRecord.getPrepTime());
                ps.setString(6, recipeRecord.getTotalTime());
                ps.setTimestamp(7, recipeRecord.getDatePublished());
                ps.setString(8, recipeRecord.getDescription());
                ps.setString(9, recipeRecord.getRecipeCategory());
                ps.setObject(10, recipeRecord.getAggregatedRating());
                ps.setInt(11, recipeRecord.getReviewCount());
                ps.setObject(12, recipeRecord.getCalories());
                ps.setObject(13, recipeRecord.getFatContent());
                ps.setObject(14, recipeRecord.getSaturatedFatContent());
                ps.setObject(15, recipeRecord.getCholesterolContent());
                ps.setObject(16, recipeRecord.getSodiumContent());
                ps.setObject(17, recipeRecord.getCarbohydrateContent());
                ps.setObject(18, recipeRecord.getFiberContent());
                ps.setObject(19, recipeRecord.getSugarContent());
                ps.setObject(20, recipeRecord.getProteinContent());
                ps.setInt(21, recipeRecord.getRecipeServings());
                ps.setString(22, recipeRecord.getRecipeYield());
            }
            @Override
            public int getBatchSize() {
                return recipeRecords.size();
            }
        });
        String insertReviewSQL = "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) VALUES (?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(insertReviewSQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ReviewRecord reviewRecord = reviewRecords.get(i);
                ps.setLong(1, reviewRecord.getReviewId());
                ps.setLong(2, reviewRecord.getRecipeId());
                ps.setLong(3, reviewRecord.getAuthorId());
                ps.setObject(4, reviewRecord.getRating());
                ps.setString(5, reviewRecord.getReview());
                ps.setTimestamp(6, reviewRecord.getDateSubmitted());
                ps.setTimestamp(7, reviewRecord.getDateModified());
            }
            @Override
            public int getBatchSize() {
                return reviewRecords.size();
            }
        });

        List<long[]> followerPairs = new ArrayList<>();
        for (UserRecord userRecord : userRecords) {
            long authorId = userRecord.getAuthorId();
            for (long followerId : userRecord.getFollowerUsers()) {
                followerPairs.add(new long[]{followerId, authorId});
            }
        }
        String insertUserFollowerSQL = "INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?)";
        jdbcTemplate.batchUpdate(insertUserFollowerSQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                long[] pair = followerPairs.get(i);
                ps.setLong(1, pair[0]);
                ps.setLong(2, pair[1]);
            }

            @Override
            public int getBatchSize() {
                return followerPairs.size();
            }
        });
        List<Object[]> ingredientPairs = new ArrayList<>();
        for (RecipeRecord recipeRecord : recipeRecords) {
            long recipeId = recipeRecord.getRecipeId();
            String[] ingredients = recipeRecord.getRecipeIngredientParts();
            Set<String> uniqueIngredients = new HashSet<>();
            for (String ingredient : ingredients) {
                uniqueIngredients.add(ingredient);
            }
            for (String uniqueIngredient : uniqueIngredients) {
                ingredientPairs.add(new Object[]{recipeId, uniqueIngredient});
            }
        }
        String recipe_ingredientsSQL = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?)";
        jdbcTemplate.batchUpdate(recipe_ingredientsSQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Object[] pair = ingredientPairs.get(i);
                ps.setLong(1, (Long) pair[0]);
                ps.setString(2, (String) pair[1]);
            }
            @Override
            public int getBatchSize() {
                return ingredientPairs.size();
            }
        });
        String review_likesSQL="INSERT INTO review_likes (ReviewId, AuthorId) VALUES (?, ?)";
        List<Object[]> likespairs=new ArrayList<>();
        for(ReviewRecord reviewRecord:reviewRecords){
            long reviewId=reviewRecord.getReviewId();
            long[] authorIds=reviewRecord.getLikes();
            for(long Id:authorIds){
                likespairs.add(new Object[]{reviewId,Id});
            }
        }
        jdbcTemplate.batchUpdate(review_likesSQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Object[] pair = likespairs.get(i);
                ps.setLong(1, (Long) pair[0]);
                ps.setLong(2, (Long) pair[1]);
            }
            @Override
            public int getBatchSize() {
                return likespairs.size();
            }
        });
    }


    private void createTables() {
        String[] createTableSQLs = {
                // 创建users表
                "CREATE TABLE IF NOT EXISTS users (" +
                        "    AuthorId BIGINT PRIMARY KEY, " +
                        "    AuthorName VARCHAR(255) NOT NULL, " +
                        "    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female')), " +
                        "    Age INTEGER CHECK (Age > 0), " +
                        "    Followers INTEGER DEFAULT 0 CHECK (Followers >= 0), " +
                        "    Following INTEGER DEFAULT 0 CHECK (Following >= 0), " +
                        "    Password VARCHAR(255), " +
                        "    IsDeleted BOOLEAN DEFAULT FALSE" +
                        ")",

                // 创建recipes表
                "CREATE TABLE IF NOT EXISTS recipes (" +
                        "    RecipeId BIGINT PRIMARY KEY, " +
                        "    Name VARCHAR(500) NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    CookTime VARCHAR(50), " +
                        "    PrepTime VARCHAR(50), " +
                        "    TotalTime VARCHAR(50), " +
                        "    DatePublished TIMESTAMP, " +
                        "    Description TEXT, " +
                        "    RecipeCategory VARCHAR(255), " +
                        "    AggregatedRating DECIMAL(3,2) CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5), " +
                        "    ReviewCount INTEGER DEFAULT 0 CHECK (ReviewCount >= 0), " +
                        "    Calories DECIMAL(10,2), " +
                        "    FatContent DECIMAL(10,2), " +
                        "    SaturatedFatContent DECIMAL(10,2), " +
                        "    CholesterolContent DECIMAL(10,2), " +
                        "    SodiumContent DECIMAL(10,2), " +
                        "    CarbohydrateContent DECIMAL(10,2), " +
                        "    FiberContent DECIMAL(10,2), " +
                        "    SugarContent DECIMAL(10,2), " +
                        "    ProteinContent DECIMAL(10,2), " +
                        "    RecipeServings VARCHAR(100), " +
                        "    RecipeYield VARCHAR(100), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建reviews表
                "CREATE TABLE IF NOT EXISTS reviews (" +
                        "    ReviewId BIGINT PRIMARY KEY, " +
                        "    RecipeId BIGINT NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Rating INTEGER, " +
                        "    Review TEXT, " +
                        "    DateSubmitted TIMESTAMP, " +
                        "    DateModified TIMESTAMP, " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建recipe_ingredients表
                "CREATE TABLE IF NOT EXISTS recipe_ingredients (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientPart VARCHAR(500), " +
                        "    PRIMARY KEY (RecipeId, IngredientPart), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId)" +
                        ")",

                // 创建review_likes表
                "CREATE TABLE IF NOT EXISTS review_likes (" +
                        "    ReviewId BIGINT, " +
                        "    AuthorId BIGINT, " +
                        "    PRIMARY KEY (ReviewId, AuthorId), " +
                        "    FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建user_follows表
                "CREATE TABLE IF NOT EXISTS user_follows (" +
                        "    FollowerId BIGINT, " +
                        "    FollowingId BIGINT, " +
                        "    PRIMARY KEY (FollowerId, FollowingId), " +
                        "    FOREIGN KEY (FollowerId) REFERENCES users(AuthorId), " +
                        "    FOREIGN KEY (FollowingId) REFERENCES users(AuthorId), " +
                        "    CHECK (FollowerId != FollowingId)" +
                        ")"
        };

        for (String sql : createTableSQLs) {
            jdbcTemplate.execute(sql);
        }
    }



    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void drop() {
        // You can use the default drop script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.
        // This method will delete all the tables in the public schema.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'DROP TABLE IF EXISTS ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
