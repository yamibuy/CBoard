package org.cboard.controller;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class LoginController {

    @RequestMapping(value = "/login", method = RequestMethod.GET)
    public String loginPage() {
        return "login";
    }
    
    @RequestMapping(value = "/timeout")
    public void sessionTimeout(HttpServletRequest request,HttpServletResponse response) throws IOException {  
        if (request.getHeader("x-requested-with") != null  
                && request.getHeader("x-requested-with").equalsIgnoreCase(  
                        "XMLHttpRequest")) { // ajax 超时处理  
            response.getWriter().print("timeout");  //设置超时标识
            response.getWriter().close();
        } else {
             response.sendRedirect("/login");  
        }
    }  

    private String getPrincipal(){
        String userName = null;
        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();

        if (principal instanceof UserDetails) {
            userName = ((UserDetails)principal).getUsername();
        } else {
            userName = principal.toString();
        }
        return userName;
    }

}