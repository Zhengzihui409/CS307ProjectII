package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UserServiceImpl implements UserService {


    private final JdbcTemplate jdbcTemplate;

    public UserServiceImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    private static final int MAX_RETRY = 3;

    @Override
    public long register(RegisterUserReq req) {

        if (req == null || !StringUtils.hasText(req.getName())
                || req.getGender() == null || req.getBirthday() == null) {
            return -1;
        }
        if (req.getGender() != RegisterUserReq.Gender.UNKNOWN &&
                req.getGender() != RegisterUserReq.Gender.MALE &&
                req.getGender() != RegisterUserReq.Gender.FEMALE) {
            return -1;
        }

        int age;
        try {
            java.time.LocalDate birthDate = java.time.LocalDate.parse(req.getBirthday());
            age = java.time.Period.between(birthDate, java.time.LocalDate.now()).getYears();
            if (age <= 0) {
                return -1;
            }
        } catch (DateTimeParseException e) {
            return -1;
        }

        String sqlCheckName = "SELECT 1 FROM users WHERE AuthorName = ?";
        try {
            jdbcTemplate.queryForObject(sqlCheckName, Integer.class, req.getName());
            return -1;
        } catch (EmptyResultDataAccessException ignored) {
        }
        String genderStr;
        if (req.getGender() == RegisterUserReq.Gender.MALE) {
            genderStr = "Male";
        } else if (req.getGender() == RegisterUserReq.Gender.FEMALE) {
            genderStr = "Female";
        } else {
            return -1;
        }

        String password = req.getPassword() == null ? "" : req.getPassword();
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                String maxIdSql = "SELECT COALESCE(MAX(AuthorId), 9853) FROM users";
                Long currentMax = jdbcTemplate.queryForObject(maxIdSql, Long.class);
                long newAuthorId = currentMax + 1;
                String sqlInsert = """
                INSERT INTO users (
                    AuthorId, AuthorName, Gender, Age, Password, IsDeleted,
                    Followers, Following
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;
                int updated = jdbcTemplate.update(sqlInsert,
                        newAuthorId,
                        req.getName(),
                        genderStr,
                        age,
                        password,
                        false,
                        0,
                        0
                );

                if (updated == 1) {
                    return newAuthorId;
                }
            } catch (DuplicateKeyException e) {
                continue;
            } catch (Exception e) {
                break;
            }
        }
        return -1;
    }
    @Override
    public long login(AuthInfo auth) {
        if (auth == null || !StringUtils.hasText(auth.getPassword())) {
            return -1;
        }
        String sql = "SELECT Password FROM users WHERE AuthorId = ? AND IsDeleted = FALSE";
        String storedPassword;
        try {
            storedPassword = jdbcTemplate.queryForObject(sql, String.class, auth.getAuthorId());
        } catch (EmptyResultDataAccessException e) {
            return -1;
        }

        if (storedPassword != null && storedPassword.equals(auth.getPassword())) {
            return auth.getAuthorId();
        } else {
            return -1;
        }
    }
    @Override
    public boolean deleteAccount(AuthInfo auth, long userId) {
        if(auth==null||auth.getAuthorId()!=userId){
            throw(new SecurityException("Invalid auth info"));
        }
        String sql="SELECT IsDeleted From users WHERE AuthorId=?";
        Boolean isDeleted;
        try {
            isDeleted = jdbcTemplate.queryForObject(
                    sql,
                    Boolean.class,
                    userId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("目标用户不存在");
        }
        if(isDeleted){
            return false;
        }
        String sqlUpdate="UPDATE users SET IsDeleted=TRUE WHERE AuthorId=?";
        jdbcTemplate.update(sqlUpdate,userId);
        String deletefollow="DELETE FROM user_follows WHERE FollowerId=? OR FollowingId=?";
        jdbcTemplate.update(deletefollow,userId,userId);

        return true;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        if (auth == null) {
            throw new SecurityException("无效凭证或不能关注自己");
        }
        Long followerId = auth.getAuthorId();
        if (followerId == null || followerId == followeeId) {
            throw new SecurityException("无效凭证或不能关注自己");
        }
        if (!userExists(followerId)) {
            throw new SecurityException("当前用户无效");
        }
        if (!userExists(followeeId)) {
            throw new SecurityException("目标用户不存在");
        }
        String checkSql = "SELECT COUNT(*) FROM user_follows WHERE FollowerId = ? AND FollowingId = ?";
        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, followerId, followeeId);

        if (count == null || count == 0) {
            String insertSql = "INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?)";
            jdbcTemplate.update(insertSql, followerId, followeeId);
            jdbcTemplate.update("UPDATE users SET Following = Following + 1 WHERE AuthorId = ?", followerId);
            jdbcTemplate.update("UPDATE users SET Followers = Followers + 1 WHERE AuthorId = ?", followeeId);

            return true;
        } else if (count == 1) {
            String deleteSql = "DELETE FROM user_follows WHERE FollowerId = ? AND FollowingId = ?";
            jdbcTemplate.update(deleteSql, followerId, followeeId);
            jdbcTemplate.update("UPDATE users SET Following = GREATEST(Following - 1, 0) WHERE AuthorId = ?", followerId);
            jdbcTemplate.update("UPDATE users SET Followers = GREATEST(Followers - 1, 0) WHERE AuthorId = ?", followeeId);

            return true;
        } else {
            return false;
        }
    }

    private boolean userExists(long userId) {
        try {
            String sql = "SELECT EXISTS(SELECT 1 FROM users WHERE AuthorId = ? AND IsDeleted = FALSE)";
            Boolean exists = jdbcTemplate.queryForObject(sql, Boolean.class, userId);
            return Boolean.TRUE.equals(exists);
        } catch (EmptyResultDataAccessException e) {
            return false;
        } catch (Exception e) {
            return false;
        }
    }


    @Override
    public UserRecord getById(long userId) {
        String userSql = "SELECT AuthorId, AuthorName, Gender, Age, Password " +
                "FROM users WHERE AuthorId = ? AND IsDeleted = FALSE";
        UserRecord user;
        try {
            user = jdbcTemplate.queryForObject(userSql, (rs, rowNum) -> {
                UserRecord ur = new UserRecord();
                ur.setAuthorId(rs.getLong("AuthorId"));
                ur.setAuthorName(rs.getString("AuthorName"));
                ur.setGender(rs.getString("Gender"));
                ur.setAge(rs.getInt("Age"));
                ur.setPassword(rs.getString("Password"));
                return ur;
            }, userId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
        int followers = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM user_follows WHERE FollowingId = ?", Integer.class, userId);
        int following = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM user_follows WHERE FollowerId = ?", Integer.class, userId);

        user.setFollowers(followers);
        user.setFollowing(following);
        List<Long> followerList = jdbcTemplate.queryForList(
                "SELECT FollowerId FROM user_follows WHERE FollowingId = ?", Long.class, userId);
        user.setFollowerUsers(followerList.stream().mapToLong(Long::longValue).toArray());

        List<Long> followingList = jdbcTemplate.queryForList(
                "SELECT FollowingId FROM user_follows WHERE FollowerId = ?", Long.class, userId);
        user.setFollowingUsers(followingList.stream().mapToLong(Long::longValue).toArray());

        return user;
    }

    private static final String SQL_UPDATE =
            "UPDATE users " +
                    "SET gender = COALESCE(?, gender), " +
                    "    age    = COALESCE(?, age) " +
                    "WHERE authorid = ? AND isdeleted = FALSE";

    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {
        if (auth == null || auth.getAuthorId() <= 0) {
            throw new SecurityException("认证信息无效，无法更新用户资料");
        }
        if (gender == null && age == null) {
            return; // 没有需要更新的字段
        }
        if (gender != null && !"Male".equals(gender) && !"Female".equals(gender)) {
            throw new IllegalArgumentException("性别参数错误，仅支持 Male 或 Female");
        }
        if (age != null && age < 0) {
            throw new IllegalArgumentException("年龄参数错误，不能为负数");
        }

        int updated = jdbcTemplate.update(SQL_UPDATE,
                gender,                 // gender 可为 null，COALESCE 保留原值
                age,                    // age 可为 null，COALESCE 保留原值
                auth.getAuthorId());

        if (updated == 0) {
            // 用户不存在或已被删除
            throw new IllegalArgumentException("目标用户不存在或已被删除");
        }
    }
    private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, String category) {
        if (auth == null || auth.getAuthorId() <= 0) {
            throw new SecurityException("认证信息无效，无法获取用户信息");
        }

        int validPage = Math.max(page, 1);
        int validSize = Math.min(Math.max(size, 1), 200);
        int offset = (validPage - 1) * validSize;

        List<Object> params = new ArrayList<>();
        params.add(auth.getAuthorId());

        StringBuilder whereClause = new StringBuilder(
                "WHERE r.AuthorId IN (SELECT FollowingId FROM user_follows WHERE FollowerId = ?)"
        );

        if (category != null && !category.trim().isEmpty()) {
            whereClause.append(" AND r.RecipeCategory = ?");
            params.add(category);
        }

        String sql = "SELECT r.RecipeId, r.Name, r.AuthorId, u.AuthorName, r.DatePublished, r.AggregatedRating, r.ReviewCount " +
                "FROM recipes r " +
                "LEFT JOIN users u ON r.AuthorId = u.AuthorId " +
                whereClause +
                " ORDER BY r.DatePublished DESC, r.RecipeId DESC " +
                "LIMIT ? OFFSET ?";

        params.add(validSize);
        params.add(offset);
        List<FeedItem> feedItems = jdbcTemplate.query(sql, (rs, rowNum) -> {
            FeedItem item = new FeedItem();
            item.setRecipeId(rs.getLong("RecipeId"));
            item.setName(rs.getString("Name"));
            item.setAuthorId(rs.getLong("AuthorId"));
            item.setAuthorName(rs.getString("AuthorName"));
            Timestamp ts = rs.getTimestamp("DatePublished", UTC_CALENDAR);
            item.setDatePublished(ts != null ? ts.toInstant() : null);

            item.setAggregatedRating(rs.getDouble("AggregatedRating"));
            item.setReviewCount(rs.getInt("ReviewCount"));
            return item;
        }, params.toArray());
        String countSql = "SELECT COUNT(*) FROM recipes r " +
                "LEFT JOIN users u ON r.AuthorId = u.AuthorId " +
                whereClause;
        List<Object> countParams = params.subList(0, params.size() - 2);
        Integer total = jdbcTemplate.queryForObject(countSql, Integer.class, countParams.toArray());
        if (total == null) total = 0;

        return PageResult.<FeedItem>builder()
                .items(feedItems)
                .page(validPage)
                .size(validSize)
                .total(total)
                .build();
    }

    @Override
    public Map<String, Object> getUserWithHighestFollowRatio() {
        String sql = """
        SELECT 
            u.AuthorId,
            u.AuthorName,
            COALESCE(f1.follower_count, 0) AS follower_count,
            COALESCE(f2.following_count, 0) AS following_count
        FROM users u
        LEFT JOIN (
            SELECT FollowingId, COUNT(*) AS follower_count 
            FROM user_follows 
            GROUP BY FollowingId
        ) f1 ON u.AuthorId = f1.FollowingId
        LEFT JOIN (
            SELECT FollowerId, COUNT(*) AS following_count 
            FROM user_follows 
            GROUP BY FollowerId
        ) f2 ON u.AuthorId = f2.FollowerId
        WHERE u.IsDeleted = FALSE 
          AND COALESCE(f2.following_count, 0) > 0
        """;

        List<Map<String, Object>> users = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> user = new HashMap<>();
            user.put("AuthorId", rs.getLong("AuthorId"));
            user.put("AuthorName", rs.getString("AuthorName"));
            user.put("FollowerCount", rs.getInt("follower_count"));
            user.put("FollowingCount", rs.getInt("following_count"));
            return user;
        });

        if (users.isEmpty()) {
            return null;
        }
        Map<String, Object> best = users.get(0);
        for (Map<String, Object> candidate : users) {
            long bestId = (Long) best.get("AuthorId");
            long candId = (Long) candidate.get("AuthorId");

            int bestFollowers = (Integer) best.get("FollowerCount");
            int bestFollowing = (Integer) best.get("FollowingCount");
            int candFollowers = (Integer) candidate.get("FollowerCount");
            int candFollowing = (Integer) candidate.get("FollowingCount");
            long candCross = (long) candFollowers * bestFollowing;
            long bestCross = (long) bestFollowers * candFollowing;

            if (candCross > bestCross) {
                best = candidate;
            } else if (candCross == bestCross && candId < bestId) {
                best = candidate;
            }
        }

        double ratio = ((int) best.get("FollowerCount")) * 1.0 / ((int) best.get("FollowingCount"));
        Map<String, Object> result = new HashMap<>();
        result.put("AuthorId", best.get("AuthorId"));
        result.put("AuthorName", best.get("AuthorName"));
        result.put("Ratio", ratio);
        return result;
    }
}