package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
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
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class RecipeServiceImpl implements RecipeService {
    private JdbcTemplate jdbcTemplate;
    @Autowired
    public RecipeServiceImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    @Override
    public String getNameFromID(long id) {
        if (id <= 0) {
            throw new IllegalArgumentException("Recipe ID must be positive");
        }
        String sql="SELECT Name FROM recipes WHERE RecipeId=?";
        try{
            return jdbcTemplate.queryForObject(
                    "SELECT name FROM recipes WHERE recipeid = ?",
                    String.class,
                    id
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
    @Override
    public RecipeRecord getRecipeById(long recipeId) {
        if (recipeId <= 0) {
            throw new IllegalArgumentException("recipeId must be positive");
        }
        String recipeSql = "SELECT " +
                "r.RecipeId, r.Name, r.AuthorId, u.AuthorName, " +
                "r.CookTime, r.PrepTime, r.TotalTime, r.DatePublished, " +
                "r.Description, r.RecipeCategory, " +
                "r.AggregatedRating, r.ReviewCount, r.Calories, " +
                "r.FatContent, r.SaturatedFatContent, r.CholesterolContent, " +
                "r.SodiumContent, r.CarbohydrateContent, r.FiberContent, " +
                "r.SugarContent, r.ProteinContent, r.RecipeServings, r.RecipeYield " +
                "FROM recipes r " +
                "LEFT JOIN users u ON r.AuthorId = u.AuthorId " +
                "WHERE r.RecipeId = ?";

        try {
            RecipeRecord recipe = jdbcTemplate.queryForObject(recipeSql, (rs, rowNum) ->
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
                                    .build(),
                    recipeId
            );
            String ingredientsSql = "SELECT IngredientPart " +
                    "FROM recipe_ingredients " +
                    "WHERE RecipeId = ? " +
                    "ORDER BY IngredientPart";
            List<String> ingredients = jdbcTemplate.queryForList(
                    ingredientsSql,
                    new Object[]{recipeId},
                    String.class
            );
            recipe.setRecipeIngredientParts(ingredients.toArray(new String[0]));
            return recipe;

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
    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating,
                                                  Integer page, Integer size, String sort) {
        if (page == null || page < 1) {
            throw new IllegalArgumentException("Page number must be at least 1");
        }
        if (size == null || size <= 0) {
            throw new IllegalArgumentException("Size must be positive");
        }

        List<Object> params = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder("WHERE 1=1");

        if (keyword != null && !keyword.trim().isEmpty()) {
            String likePattern = "%" + keyword.trim().toLowerCase() + "%";
            whereClause.append(" AND (LOWER(r.name) LIKE ? OR LOWER(r.description) LIKE ?)");
            params.add(likePattern);
            params.add(likePattern);
        }
        if (category != null && !category.trim().isEmpty()) {
            whereClause.append(" AND r.recipecategory = ?");
            params.add(category.trim());
        }
        if (minRating != null) {
            whereClause.append(" AND r.aggregatedrating >= ?");
            params.add(minRating);
        }
        String orderBy = "ORDER BY r.aggregatedrating DESC, r.recipeid DESC";
        if (sort != null) {
            switch (sort) {
                case "rating_desc":
                    orderBy = "ORDER BY r.aggregatedrating DESC, r.recipeid DESC";
                    break;
                case "date_desc":
                    orderBy = "ORDER BY r.datepublished DESC, r.recipeid DESC";
                    break;
                case "calories_asc":
                    orderBy = "ORDER BY r.calories ASC, r.recipeid ASC";
                    break;
            }
        }
        String countSql = "SELECT COUNT(*) FROM recipes r " + whereClause;
        long total = jdbcTemplate.queryForObject(countSql, Long.class, params.toArray());
        int offset = (page - 1) * size;
        String dataSql = "SELECT " +
                "r.recipeid, r.name, r.authorid, u.authorname, " +
                "r.cooktime, r.preptime, r.totaltime, r.datepublished, " +
                "r.description, r.recipecategory, " +
                "r.aggregatedrating, r.reviewcount, r.calories, " +
                "r.fatcontent, r.saturatedfatcontent, r.cholesterolcontent, " +
                "r.sodiumcontent, r.carbohydratecontent, r.fibercontent, " +
                "r.sugarcontent, r.proteincontent, r.recipeservings, r.recipeyield " +
                "FROM recipes r " +
                "LEFT JOIN users u ON r.authorid = u.authorid " +
                whereClause + " " + orderBy + " LIMIT ? OFFSET ?";

        params.add(size);
        params.add(offset);

        List<RecipeRecord> records = jdbcTemplate.query(dataSql, params.toArray(), (rs, rowNum) ->
                RecipeRecord.builder()
                        .RecipeId(rs.getLong("recipeid"))
                        .name(rs.getString("name"))
                        .authorId(rs.getLong("authorid"))
                        .authorName(rs.getString("authorname"))
                        .cookTime(rs.getString("cooktime"))
                        .prepTime(rs.getString("preptime"))
                        .totalTime(rs.getString("totaltime"))
                        .datePublished(rs.getTimestamp("datepublished"))
                        .description(rs.getString("description"))
                        .recipeCategory(rs.getString("recipecategory"))
                        .aggregatedRating(rs.getFloat("aggregatedrating"))
                        .reviewCount(rs.getInt("reviewcount"))
                        .calories(rs.getFloat("calories"))
                        .fatContent(rs.getFloat("fatcontent"))
                        .saturatedFatContent(rs.getFloat("saturatedfatcontent"))
                        .cholesterolContent(rs.getFloat("cholesterolcontent"))
                        .sodiumContent(rs.getFloat("sodiumcontent"))
                        .carbohydrateContent(rs.getFloat("carbohydratecontent"))
                        .fiberContent(rs.getFloat("fibercontent"))
                        .sugarContent(rs.getFloat("sugarcontent"))
                        .proteinContent(rs.getFloat("proteincontent"))
                        .recipeServings(parseRecipeServings(rs.getString("recipeservings")))
                        .recipeYield(rs.getString("recipeyield"))
                        .recipeIngredientParts(getIngredientsByRecipeId(rs.getLong("recipeid")))
                        .build()
        );

        return PageResult.<RecipeRecord>builder()
                .total(total)
                .page(page)
                .size(size)
                .items(records)
                .build();
    }
    private String[] getIngredientsByRecipeId(long recipeId) {
        String sql = "SELECT ingredientpart FROM recipe_ingredients WHERE recipeid = ? ORDER BY LOWER(ingredientpart)";
        List<String> ingredients = jdbcTemplate.queryForList(sql, String.class, recipeId);
        return ingredients.toArray(new String[0]);
    }

    @Override
    public long createRecipe(RecipeRecord dto, AuthInfo auth) {
        if (auth == null || auth.getAuthorId() != dto.getAuthorId()) {
            throw new SecurityException("Invalid auth info");
        }
        if (dto.getName() == null || dto.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("Name cannot be empty");
        }
        String userCheckSql = "SELECT AuthorId FROM users WHERE AuthorId = ? AND IsDeleted = false";
        try {
            Long existingUserId = jdbcTemplate.queryForObject(userCheckSql, Long.class, auth.getAuthorId());
            if (existingUserId == null) {
                throw new SecurityException("User does not exist or is deleted");
            }
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("User does not exist or is deleted");
        }
        Long currentMaxId = jdbcTemplate.queryForObject(
                "SELECT COALESCE(MAX(recipeid), 0) FROM recipes",
                Long.class
        );
        long newRecipeId = currentMaxId + 1;
        String insertSql = "INSERT INTO recipes (" +
                "recipeid, Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, " +
                "Description, RecipeCategory, AggregatedRating, ReviewCount, " +
                "Calories, FatContent, SaturatedFatContent, CholesterolContent, " +
                "SodiumContent, CarbohydrateContent, FiberContent, SugarContent, " +
                "ProteinContent, RecipeServings, RecipeYield" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.update(insertSql,
                newRecipeId,
                dto.getName(),
                dto.getAuthorId(),
                dto.getCookTime(),
                dto.getPrepTime(),
                dto.getTotalTime(),
                dto.getDatePublished() != null ? dto.getDatePublished() : new Timestamp(System.currentTimeMillis()),
                dto.getDescription(),
                dto.getRecipeCategory(),
                dto.getAggregatedRating(),
                dto.getReviewCount(),
                dto.getCalories(),
                dto.getFatContent(),
                dto.getSaturatedFatContent(),
                dto.getCholesterolContent(),
                dto.getSodiumContent(),
                dto.getCarbohydrateContent(),
                dto.getFiberContent(),
                dto.getSugarContent(),
                dto.getProteinContent(),
                dto.getRecipeServings(),
                dto.getRecipeYield()
        );
        if (dto.getRecipeIngredientParts() != null) {
            String insertSqlIng = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?)";
            Set<String> uniqueIngredients = new HashSet<>(Arrays.asList(dto.getRecipeIngredientParts()));
            for (String ingredient : uniqueIngredients) {
                if (ingredient != null && !ingredient.trim().isEmpty()) {
                    jdbcTemplate.update(insertSqlIng, newRecipeId, ingredient);
                }
            }
        }
        return newRecipeId;
    }


    @Override
    public void deleteRecipe(long recipeId, AuthInfo auth) {
        if(auth==null){
            throw(new SecurityException("Invalid auth info"));
        }
        String query="SELECT AuthorId FROM recipes WHERE RecipeId=?";
        long authorId;
        try {
            authorId = jdbcTemplate.queryForObject(query, new Object[]{recipeId}, Long.class);
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("Recipe not found or no permission to delete", e);
        }
        if(auth.getAuthorId()!=authorId){
            throw(new SecurityException("Invalid auth info"));
        }
        String deleteSql3 = "DELETE FROM reviews WHERE RecipeId = ?";
        jdbcTemplate.update(deleteSql3, recipeId);
        String deleteSql2 = "DELETE FROM recipe_ingredients WHERE RecipeId = ?";
        jdbcTemplate.update(deleteSql2, recipeId);
        String deleteSql = "DELETE FROM recipes WHERE RecipeId = ?";
        jdbcTemplate.update(deleteSql, recipeId);
    }

    @Override
    public void updateTimes(AuthInfo auth, long recipeId, String cookTimeIso, String prepTimeIso) {
        if (auth == null) {
            throw new SecurityException("Invalid auth info");
        }
        String query = "SELECT AuthorId FROM recipes WHERE RecipeId = ?";
        Long authorId;
        try {
            authorId = jdbcTemplate.queryForObject(query, Long.class, recipeId);
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("Recipe not found or no permission to update", e);
        }
        if (!authorId.equals(auth.getAuthorId())) {
            throw new SecurityException("Invalid auth info");
        }
        Duration newCookDuration = Duration.ZERO;
        if (cookTimeIso != null) {
            try {
                newCookDuration = Duration.parse(cookTimeIso);
                if (newCookDuration.isNegative()) {
                    throw new IllegalArgumentException("Cook time cannot be negative");
                }
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid ISO 8601 cook time format", e);
            }
        }
        Duration newPrepDuration = Duration.ZERO;
        if (prepTimeIso != null) {
            try {
                newPrepDuration = Duration.parse(prepTimeIso);
                if (newPrepDuration.isNegative()) {
                    throw new IllegalArgumentException("Prep time cannot be negative");
                }
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid ISO 8601 prep time format", e);
            }
        }
        Duration totalDuration = newCookDuration.plus(newPrepDuration);
        String updateSql = "UPDATE recipes SET CookTime = ?, PrepTime = ?, TotalTime = ? WHERE RecipeId = ?";
        jdbcTemplate.update(updateSql,
                newCookDuration.toString(),
                newPrepDuration.toString(),
                totalDuration.toString(),
                recipeId
        );
    }

    @Override
    public Map<String, Object> getClosestCaloriePair() {
        String sql = "WITH sorted_recipes AS (" +
                "    SELECT " +
                "        RecipeId, " +
                "        Calories, " +
                "        LEAD(RecipeId) OVER (ORDER BY Calories ASC, RecipeId ASC) AS NextRecipeId, " +
                "        LEAD(Calories) OVER (ORDER BY Calories ASC, RecipeId ASC) AS NextCalories " +
                "    FROM recipes " +
                "    WHERE Calories IS NOT NULL" +
                "), " +
                "calorie_diffs AS (" +
                "    SELECT " +
                "        RecipeId AS RecipeA, " +
                "        NextRecipeId AS RecipeB, " +
                "        Calories AS CaloriesA, " +
                "        NextCalories AS CaloriesB, " +
                "        ABS(Calories - NextCalories) AS Difference " +
                "    FROM sorted_recipes " +
                "    WHERE NextRecipeId IS NOT NULL" +
                ") " +
                "SELECT RecipeA, RecipeB, CaloriesA, CaloriesB, Difference " +
                "FROM calorie_diffs " +
                "ORDER BY Difference ASC, RecipeA ASC, RecipeB ASC " +
                "LIMIT 1";

        try {
            Map<String, Object> result = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Map<String, Object> pair = new HashMap<>();
                pair.put("RecipeA", rs.getLong("RecipeA"));
                pair.put("RecipeB", rs.getLong("RecipeB"));
                pair.put("CaloriesA", rs.getDouble("CaloriesA"));
                pair.put("CaloriesB", rs.getDouble("CaloriesB"));
                pair.put("Difference", rs.getDouble("Difference"));
                return pair;
            });
            return result;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<Map<String, Object>> getTop3MostComplexRecipesByIngredients() {
        String sql = """
        SELECT 
            r.recipeid AS RecipeId,
            r.name AS Name,
            COUNT(ri.ingredientpart) AS IngredientCount
        FROM recipes r
        INNER JOIN recipe_ingredients ri ON r.recipeid = ri.recipeid
        WHERE ri.ingredientpart IS NOT NULL
        GROUP BY r.recipeid, r.name
        ORDER BY IngredientCount DESC, r.recipeid ASC
        LIMIT 3
        """;

        try {
            return jdbcTemplate.query(sql, (rs, rowNum) -> {
                Map<String, Object> recipe = new HashMap<>();
                recipe.put("RecipeId", rs.getLong("RecipeId"));
                recipe.put("Name", rs.getString("Name") == null ? "未知名称" : rs.getString("Name"));
                recipe.put("IngredientCount", rs.getInt("IngredientCount"));
                return recipe;
            });
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        } catch (Exception e) {
            throw new RuntimeException("获取前3个最复杂食谱失败", e);
        }
    }


}