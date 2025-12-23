package io.sustc.controller;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.ReviewService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/reviews")
public class ReviewController {

    private final ReviewService reviewService;
    public ReviewController(ReviewService reviewService) { this.reviewService = reviewService; }

    @PostMapping
    public long add(@RequestParam long recipeId,
                    @RequestParam int rating,
                    @RequestParam(required = false) String review,
                    @RequestBody AuthInfo auth) {
        return reviewService.addReview(auth, recipeId, rating, review);
    }

    @PostMapping("/{reviewId}/edit")
    public void edit(@PathVariable long reviewId,
                     @RequestParam long recipeId,
                     @RequestParam int rating,
                     @RequestParam(required = false) String review,
                     @RequestBody AuthInfo auth) {
        reviewService.editReview(auth, recipeId, reviewId, rating, review);
    }

    @PostMapping("/{reviewId}/delete")
    public void delete(@PathVariable long reviewId,
                       @RequestParam long recipeId,
                       @RequestBody AuthInfo auth) {
        reviewService.deleteReview(auth, recipeId, reviewId);
    }

    @PostMapping("/{reviewId}/like")
    public long like(@PathVariable long reviewId, @RequestBody AuthInfo auth) {
        return reviewService.likeReview(auth, reviewId);
    }

    @PostMapping("/{reviewId}/unlike")
    public long unlike(@PathVariable long reviewId, @RequestBody AuthInfo auth) {
        return reviewService.unlikeReview(auth, reviewId);
    }

    @GetMapping("/by-recipe/{recipeId}")
    public PageResult<ReviewRecord> listByRecipe(@PathVariable long recipeId,
                                                 @RequestParam(defaultValue = "1") int page,
                                                 @RequestParam(defaultValue = "10") int size,
                                                 @RequestParam(defaultValue = "date_desc") String sort) {
        return reviewService.listByRecipe(recipeId, page, size, sort);
    }
}