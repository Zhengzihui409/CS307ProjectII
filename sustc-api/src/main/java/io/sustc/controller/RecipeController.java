package io.sustc.controller;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.RecipeService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/recipes")
public class RecipeController {

    private final RecipeService recipeService;
    public RecipeController(RecipeService recipeService) { this.recipeService = recipeService; }

    @GetMapping("/{id}")
    public RecipeRecord get(@PathVariable long id) {
        return recipeService.getRecipeById(id);
    }

    @GetMapping("/search")
    public PageResult<RecipeRecord> search(
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) Double minRating,
            @RequestParam(defaultValue = "1") Integer page,
            @RequestParam(defaultValue = "10") Integer size,
            @RequestParam(required = false) String sort
    ) {
        return recipeService.searchRecipes(keyword, category, minRating, page, size, sort);
    }

    @PostMapping
    public long create(@RequestBody RecipeRecord dto,
                       @RequestParam long authorId,
                       @RequestParam String password) {
        AuthInfo auth = new AuthInfo(authorId, password);
        return recipeService.createRecipe(dto, auth);
    }

    @PostMapping("/{id}/delete")
    public void delete(@PathVariable long id, @RequestBody AuthInfo auth) {
        recipeService.deleteRecipe(id, auth);
    }

    @PostMapping("/{id}/times")
    public void updateTimes(@PathVariable long id,
                            @RequestBody Map<String, String> body,
                            @RequestParam long authorId,
                            @RequestParam String password) {
        AuthInfo auth = new AuthInfo(authorId, password);
        recipeService.updateTimes(auth, id, body.get("cookTimeIso"), body.get("prepTimeIso"));
    }

    @GetMapping("/calories/closest-pair")
    public Map<String, Object> closestPair() {
        return recipeService.getClosestCaloriePair();
    }

    @GetMapping("/ingredients/top3")
    public java.util.List<Map<String, Object>> top3ByIngredients() {
        return recipeService.getTop3MostComplexRecipesByIngredients();
    }
}