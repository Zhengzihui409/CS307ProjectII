package io.sustc.controller;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserRecord;
import io.sustc.service.UserService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/users")
public class UserController {

    private final UserService userService;
    public UserController(UserService userService) { this.userService = userService; }

    @PostMapping("/register")
    public long register(@RequestBody RegisterUserReq req) {
        return userService.register(req);
    }

    @PostMapping("/login")
    public long login(@RequestBody AuthInfo auth) {
        return userService.login(auth);
    }

    @GetMapping("/{id}")
    public UserRecord getUser(@PathVariable long id) {
        return userService.getById(id);
    }

    @PostMapping("/{id}/delete")
    public boolean delete(@PathVariable long id, @RequestBody AuthInfo auth) {
        return userService.deleteAccount(auth, id);
    }

    @PostMapping("/{id}/follow")
    public boolean follow(@PathVariable long id, @RequestBody AuthInfo auth) {
        return userService.follow(auth, id);
    }

    @PostMapping("/{id}/profile")
    public void updateProfile(@PathVariable long id,
                              @RequestBody UserRecord body,
                              @RequestParam long authorId,
                              @RequestParam String password) {
        // 只允许本人修改，简化校验：authorId 必须等于路径 id
        if (authorId != id) throw new SecurityException("只能修改自己的资料");
        AuthInfo auth = new AuthInfo(authorId, password);
        userService.updateProfile(auth, body.getGender(), body.getAge());
    }
}