package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.RecipeService;
import io.sustc.service.ReviewService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;

@Service
@Slf4j
public class ReviewServiceImpl implements ReviewService {

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Override
    @Transactional
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {
        if (auth == null) {
            throw new IllegalArgumentException("Invalid auth information");
        }
        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Invalid rating");
        }
        String userCheckSql = "SELECT AuthorId FROM users WHERE AuthorId = ? AND IsDeleted = false";
        Long userId;
        try {
            userId = jdbcTemplate.queryForObject(userCheckSql, Long.class, auth.getAuthorId());
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("User is invalid or inactive");
        }
        String recipeCheckSql = "SELECT 1 FROM recipes WHERE RecipeId = ?";
        try {
            jdbcTemplate.queryForObject(recipeCheckSql, Integer.class, recipeId);
        } catch (EmptyResultDataAccessException e) {
            return -1;
        }
        long reviewId = generateNextReviewId();
        Timestamp now = Timestamp.from(Instant.now());
        String insertSql = """
        INSERT INTO reviews (
            ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """;
        jdbcTemplate.update(insertSql,
                reviewId,
                recipeId,
                userId,
                rating,
                review,
                now,
                now
        );

        refreshRecipeAggregatedRating(recipeId);
        return reviewId;
    }
    private long generateNextReviewId() {
        String sql = "SELECT COALESCE(MAX(ReviewId), 0) FROM reviews";
        Long maxId = jdbcTemplate.queryForObject(sql, Long.class);
        return (maxId == null ? 0L : maxId) + 1;
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        if (auth == null) {
            throw new IllegalArgumentException("Invalid auth information");
        }
        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Invalid rating");
        }

        String userCheckSql = "SELECT AuthorId FROM users WHERE AuthorId = ? AND IsDeleted = false";
        Long userId;
        try {
            userId = jdbcTemplate.queryForObject(userCheckSql, Long.class, auth.getAuthorId());
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("User is invalid or inactive");
        }
        String recipeCheckSql = "SELECT COUNT(*) FROM recipes WHERE RecipeId = ?";
        Integer recipeCount = jdbcTemplate.queryForObject(recipeCheckSql, Integer.class, recipeId);
        if (recipeCount == null || recipeCount == 0) {
            throw new IllegalArgumentException("Recipe not found");
        }
        String reviewCheckSql = "SELECT AuthorId FROM reviews WHERE ReviewId = ? AND RecipeId = ?";
        Long reviewAuthorId;
        try {
            reviewAuthorId = jdbcTemplate.queryForObject(reviewCheckSql, Long.class, reviewId, recipeId);
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Review not found");
        }

        if (!reviewAuthorId.equals(auth.getAuthorId())) {
            throw new SecurityException("You can only edit your own reviews");
        }

        String updateSql = "UPDATE reviews SET Rating = ?, Review = ?, DateModified = ? WHERE ReviewId = ?";
        int updated = jdbcTemplate.update(updateSql, rating, review, Timestamp.from(Instant.now()), reviewId);

        if (updated == 0) {
            throw new IllegalArgumentException("Review not found");
        }
        refreshRecipeAggregatedRating(recipeId);
    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        if (auth == null) {
            throw new IllegalArgumentException("Invalid auth information");
        }

        String userCheckSql = "SELECT AuthorId FROM users WHERE AuthorId = ? AND IsDeleted = false";
        Long userId;
        try {
            userId = jdbcTemplate.queryForObject(userCheckSql, Long.class, auth.getAuthorId());
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("User is invalid or inactive");
        }
        String query = "SELECT AuthorId FROM reviews WHERE RecipeId = ? AND ReviewId = ?";
        List<Long> authors = jdbcTemplate.queryForList(query, Long.class, recipeId, reviewId);

        if (authors.isEmpty()) {
            throw new IllegalArgumentException("Review not found or does not belong to the specified recipe");
        }

        long authorId = authors.get(0);
        if (authorId != userId) {
            throw new SecurityException("Not allowed to delete this review");
        }

        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId = ?", reviewId);
        jdbcTemplate.update("DELETE FROM reviews WHERE ReviewId = ?", reviewId);
        refreshRecipeAggregatedRating(recipeId);
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        if (auth == null || auth.getPassword() == null) {
            throw new IllegalArgumentException("Invalid auth information");
        }
        String userCheckSql = "SELECT AuthorId FROM users WHERE AuthorId = ? AND Password = ? AND IsDeleted = false";
        Long userId;
        try {
            userId = jdbcTemplate.queryForObject(userCheckSql, Long.class, auth.getAuthorId(), auth.getPassword());
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("Invalid credentials or user inactive");
        }
        String checkReviewSql = "SELECT AuthorId FROM reviews WHERE ReviewId = ?";
        Long reviewAuthorId;
        try {
            reviewAuthorId = jdbcTemplate.queryForObject(checkReviewSql, Long.class, reviewId);
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Review does not exist");
        }
        if (Objects.equals(reviewAuthorId, userId)) {
            throw new IllegalArgumentException("Cannot like your own review");
        }
        String checkLikeSql = "SELECT COUNT(*) FROM review_likes WHERE ReviewId = ? AND AuthorId = ?";
        Integer likeCount = jdbcTemplate.queryForObject(checkLikeSql, Integer.class, reviewId, userId);
        if (likeCount == null || likeCount == 0) {
            jdbcTemplate.update("INSERT INTO review_likes (ReviewId, AuthorId) VALUES (?, ?)", reviewId, userId);
        }

        String countSql = "SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?";
        Integer totalLikes = jdbcTemplate.queryForObject(countSql, Integer.class, reviewId);
        return totalLikes != null ? totalLikes : 0L;
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        if (auth == null || auth.getAuthorId() <= 0) {
            throw new IllegalArgumentException("Invalid auth info: authorId is required");
        }
        String userCheckSql = "SELECT AuthorId FROM users WHERE AuthorId = ? AND Password = ? AND IsDeleted = false";
        Long userId;
        try {
            userId = jdbcTemplate.queryForObject(userCheckSql, Long.class, auth.getAuthorId(), auth.getPassword());
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("Invalid credentials or user inactive");
        }
        String checkReviewSql = "SELECT 1 FROM reviews WHERE ReviewId = ?";
        try {
            jdbcTemplate.queryForObject(checkReviewSql, Integer.class, reviewId);
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Review does not exist");
        }
        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId = ? AND AuthorId = ?", reviewId, userId);
        String likesql="SELECT COUNT(*) FROM review_likes WHERE ReviewId=?";
        int likes = jdbcTemplate.queryForObject(likesql, Integer.class, reviewId);
        return likes;
    }

    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        String checkRecipeSql = "SELECT 1 FROM recipes WHERE RecipeId = ? LIMIT 1";
        try {
            jdbcTemplate.queryForObject(checkRecipeSql, Integer.class, recipeId);
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe does not exist");
        }
        if(page <1||size<=0){
            throw new IllegalArgumentException("Invalid page or size");
        }
        String orderBy = "ORDER BY DateModified DESC";
        if ("likes_desc".equals(sort)) {
            orderBy = "ORDER BY (SELECT COUNT(*) FROM review_likes WHERE ReviewId = r.ReviewId) DESC, DateModified DESC";
        }
        String count="SELECT COUNT(*) FROM reviews WHERE RecipeId = ?";
        long total = jdbcTemplate.queryForObject(count, Long.class, recipeId);
        int offset = (page - 1) * size;
        String dataSql = "SELECT r.ReviewId, r.RecipeId, r.AuthorId, u.AuthorName, " +
                "r.Rating, r.Review, r.DateSubmitted, r.DateModified, " +
                "(SELECT ARRAY_AGG(AuthorId) FROM review_likes WHERE ReviewId = r.ReviewId) AS likes " +
                "FROM reviews r " +
                "LEFT JOIN users u ON r.AuthorId = u.AuthorId " +
                "WHERE r.RecipeId = ? " +
                orderBy + " LIMIT ? OFFSET ?";
        List<ReviewRecord> reviews=jdbcTemplate.query(dataSql, (rs, rowNum) ->{
            ReviewRecord reviewRecord=new ReviewRecord();
            reviewRecord.setReviewId(rs.getLong("ReviewId"));
            reviewRecord.setRecipeId(rs.getLong("RecipeId"));
            reviewRecord.setAuthorId(rs.getLong("AuthorId"));
            reviewRecord.setAuthorName(rs.getString("AuthorName"));
            reviewRecord.setRating(rs.getFloat("Rating"));
            reviewRecord.setReview(rs.getString("Review"));
            reviewRecord.setDateSubmitted(rs.getTimestamp("DateSubmitted"));
            reviewRecord.setDateModified(rs.getTimestamp("DateModified"));
            Array likesArray = rs.getArray("likes");
            if (likesArray != null) {
                Object[] array = (Object[]) likesArray.getArray();
                long[] likes = new long[array.length];
                for (int i = 0; i < array.length; i++) {
                    Number num = (Number) array[i];
                    likes[i] = num.longValue();
                }
                reviewRecord.setLikes(likes);
            } else {
                reviewRecord.setLikes(new long[0]);
            }
            return reviewRecord;
        }, recipeId, size, offset);
        return PageResult.<ReviewRecord>builder()
                .total(total)
                .page(page)
                .size(size)
                .items(reviews)
                .build();
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        Boolean exists = jdbcTemplate.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM recipes WHERE RecipeId = ?)",
                Boolean.class,
                recipeId
        );
        if (!Boolean.TRUE.equals(exists)) {
            throw new IllegalArgumentException("Recipe does not exist");
        }
        String statsSql = "SELECT COUNT(*) AS review_count, AVG(Rating) AS avg_rating " +
                "FROM reviews WHERE RecipeId = ?";
        Map<String, Object> stats = jdbcTemplate.queryForMap(statsSql, recipeId);
        int reviewCount = ((Number) stats.get("review_count")).intValue();
        Object avgObj = stats.get("avg_rating");

        float finalAvgRating = 0.0f;
        if (reviewCount > 0 && avgObj != null) {
            double avg = 0.0;
            if (avgObj instanceof BigDecimal) {
                avg = ((BigDecimal) avgObj).doubleValue();
            } else if (avgObj instanceof Double) {
                avg = (Double) avgObj;
            } else {
                avg = Double.parseDouble(avgObj.toString());
            }
            finalAvgRating = (float) (Math.round(avg * 100.0) / 100.0);

        }

        jdbcTemplate.update(
                "UPDATE recipes SET AggregatedRating = ?, ReviewCount = ? WHERE RecipeId = ?",
                finalAvgRating,
                reviewCount,
                recipeId
        );
        String dataSql = """
        SELECT r.RecipeId, r.Name, r.AuthorId, u.AuthorName,
               r.CookTime, r.PrepTime, r.TotalTime, r.DatePublished, r.Description, r.RecipeCategory,
               r.AggregatedRating, r.ReviewCount, r.Calories, r.FatContent, r.SaturatedFatContent,
               r.CholesterolContent, r.SodiumContent, r.CarbohydrateContent, r.FiberContent,
               r.SugarContent, r.ProteinContent, r.RecipeServings, r.RecipeYield
        FROM recipes r
        LEFT JOIN users u ON r.AuthorId = u.AuthorId
        WHERE r.RecipeId = ?
        """;

        try {
            return jdbcTemplate.queryForObject(dataSql, new Object[]{recipeId}, (rs, rowNum) ->
                    RecipeRecord.builder()
                            .RecipeId(rs.getLong("RecipeId"))
                            .name(rs.getString("Name"))
                            .authorId(rs.getLong("AuthorId"))
                            .authorName(rs.getString("AuthorName"))
                            .cookTime(rs.getString("CookTime"))
                            .prepTime(rs.getString("PrepTime"))
                            .totalTime(rs.getString("TotalTime"))
                            .datePublished(rs.getTimestamp("DatePublished"))
                            .description(rs.getString("Description"))
                            .recipeCategory(rs.getString("RecipeCategory"))
                            .aggregatedRating(rs.getFloat("AggregatedRating"))
                            .reviewCount(rs.getInt("ReviewCount"))
                            .calories(rs.getFloat("Calories"))
                            .fatContent(rs.getFloat("FatContent"))
                            .saturatedFatContent(rs.getFloat("SaturatedFatContent"))
                            .cholesterolContent(rs.getFloat("CholesterolContent"))
                            .sodiumContent(rs.getFloat("SodiumContent"))
                            .carbohydrateContent(rs.getFloat("CarbohydrateContent"))
                            .fiberContent(rs.getFloat("FiberContent"))
                            .sugarContent(rs.getFloat("SugarContent"))
                            .proteinContent(rs.getFloat("ProteinContent"))
                            .recipeServings(parseRecipeServings(rs.getString("RecipeServings")))
                            .recipeYield(rs.getString("RecipeYield"))
                            .recipeIngredientParts(getIngredientsByRecipeId(rs.getLong("RecipeId")))
                            .build()
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
    private int parseRecipeServings(String recipeServings) {
        if (recipeServings == null || recipeServings.trim().isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(recipeServings.trim());
        } catch (NumberFormatException e) {
            String numericPart = recipeServings.replaceAll("[^0-9]", "");
            return numericPart.isEmpty() ? 0 : Integer.parseInt(numericPart);
        }
    }
    private String[] getIngredientsByRecipeId(long recipeId) {
        String sql = "SELECT IngredientPart FROM recipe_ingredients WHERE RecipeId = ? ORDER BY LOWER(IngredientPart)";
        List<String> ingredients = jdbcTemplate.queryForList(sql, String.class, recipeId);
        return ingredients.toArray(new String[0]);
    }
}