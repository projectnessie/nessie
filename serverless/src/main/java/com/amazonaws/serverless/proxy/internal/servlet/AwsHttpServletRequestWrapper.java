/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
 * with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.serverless.proxy.internal.servlet;

import static com.amazonaws.serverless.proxy.internal.servlet.AwsProxyHttpServletRequest.cleanUri;

import com.amazonaws.serverless.proxy.internal.LambdaContainerHandler;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;
import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;

public class AwsHttpServletRequestWrapper implements HttpServletRequest {

  private final HttpServletRequest originalRequest;
  private final String newPath;
  private ServletContext ctx;

  public AwsHttpServletRequestWrapper(HttpServletRequest req, String path) {
    originalRequest = req;
    newPath = path;
    ctx = originalRequest.getServletContext();
  }

  @Override
  public String getAuthType() {
    return originalRequest.getAuthType();
  }

  @Override
  public Cookie[] getCookies() {
    return originalRequest.getCookies();
  }

  @Override
  public long getDateHeader(String s) {
    return originalRequest.getDateHeader(s);
  }

  @Override
  public String getHeader(String s) {
    return originalRequest.getHeader(s);
  }

  @Override
  public Enumeration<String> getHeaders(String s) {
    return originalRequest.getHeaders(s);
  }

  @Override
  public Enumeration<String> getHeaderNames() {
    return originalRequest.getHeaderNames();
  }

  @Override
  public int getIntHeader(String s) {
    return originalRequest.getIntHeader(s);
  }

  @Override
  public String getMethod() {
    return originalRequest.getMethod();
  }

  @Override
  public String getPathInfo() {
    String pathInfo = cleanUri(newPath);
    return AwsHttpServletRequest.decodeRequestPath(pathInfo,
                                                   LambdaContainerHandler.getContainerConfig());
  }

  @Override
  public String getPathTranslated() {
    return originalRequest.getPathTranslated();
  }

  @Override
  public String getContextPath() {
    return originalRequest.getContextPath();
  }

  @Override
  public String getQueryString() {
    return originalRequest.getQueryString();
  }

  @Override
  public String getRemoteUser() {
    return originalRequest.getRemoteUser();
  }

  @Override
  public boolean isUserInRole(String s) {
    return originalRequest.isUserInRole(s);
  }

  @Override
  public Principal getUserPrincipal() {
    return originalRequest.getUserPrincipal();
  }

  @Override
  public String getRequestedSessionId() {
    return originalRequest.getRequestedSessionId();
  }

  @Override
  public String getRequestURI() {
    return cleanUri(getContextPath()) + cleanUri(newPath);
  }

  @Override
  public StringBuffer getRequestURL() {
    String url = "";
    url += getServerName();
    url += cleanUri(getContextPath());
    url += cleanUri(newPath);

    return new StringBuffer(getScheme() + "://" + url);
  }

  @Override
  public String getServletPath() {
    return originalRequest.getServletPath();
  }

  @Override
  public HttpSession getSession(boolean b) {
    return originalRequest.getSession(b);
  }

  @Override
  public HttpSession getSession() {
    return originalRequest.getSession();
  }

  @Override
  public String changeSessionId() {
    return originalRequest.changeSessionId();
  }

  @Override
  public boolean isRequestedSessionIdValid() {
    return originalRequest.isRequestedSessionIdValid();
  }

  @Override
  public boolean isRequestedSessionIdFromCookie() {
    return originalRequest.isRequestedSessionIdFromCookie();
  }

  @Override
  public boolean isRequestedSessionIdFromURL() {
    return originalRequest.isRequestedSessionIdFromURL();
  }

  @Override
  public boolean isRequestedSessionIdFromUrl() {
    return originalRequest.isRequestedSessionIdFromUrl();
  }

  @Override
  public boolean authenticate(HttpServletResponse httpServletResponse)
    throws IOException, ServletException {
    return originalRequest.authenticate(httpServletResponse);
  }

  @Override
  public void login(String s, String s1) throws ServletException {
    originalRequest.login(s, s1);
  }

  @Override
  public void logout() throws ServletException {
    originalRequest.logout();
  }

  @Override
  public Collection<Part> getParts() throws IOException, ServletException {
    return originalRequest.getParts();
  }

  @Override
  public Part getPart(String s) throws IOException, ServletException {
    return originalRequest.getPart(s);
  }

  @Override
  public <T extends HttpUpgradeHandler> T upgrade(Class<T> aClass)
    throws IOException, ServletException {
    return originalRequest.upgrade(aClass);
  }

  @Override
  public Object getAttribute(String s) {
    return originalRequest.getAttribute(s);
  }

  @Override
  public Enumeration<String> getAttributeNames() {
    return originalRequest.getAttributeNames();
  }

  @Override
  public String getCharacterEncoding() {
    return originalRequest.getCharacterEncoding();
  }

  @Override
  public void setCharacterEncoding(String s) throws UnsupportedEncodingException {
    originalRequest.setCharacterEncoding(s);
  }

  @Override
  public int getContentLength() {
    return originalRequest.getContentLength();
  }

  @Override
  public long getContentLengthLong() {
    return originalRequest.getContentLengthLong();
  }

  @Override
  public String getContentType() {
    return originalRequest.getContentType();
  }

  @Override
  public ServletInputStream getInputStream() throws IOException {
    return originalRequest.getInputStream();
  }

  @Override
  public String getParameter(String s) {
    return originalRequest.getParameter(s);
  }

  @Override
  public Enumeration<String> getParameterNames() {
    return originalRequest.getParameterNames();
  }

  @Override
  public String[] getParameterValues(String s) {
    return originalRequest.getParameterValues(s);
  }

  @Override
  public Map<String, String[]> getParameterMap() {
    return originalRequest.getParameterMap();
  }

  @Override
  public String getProtocol() {
    return originalRequest.getProtocol();
  }

  @Override
  public String getScheme() {
    return originalRequest.getScheme();
  }

  @Override
  public String getServerName() {
    return originalRequest.getServerName();
  }

  @Override
  public int getServerPort() {
    return originalRequest.getServerPort();
  }

  @Override
  public BufferedReader getReader() throws IOException {
    return originalRequest.getReader();
  }

  @Override
  public String getRemoteAddr() {
    return originalRequest.getRemoteAddr();
  }

  @Override
  public String getRemoteHost() {
    return originalRequest.getRemoteAddr();
  }

  @Override
  public void setAttribute(String s, Object o) {
    originalRequest.setAttribute(s, o);
  }

  @Override
  public void removeAttribute(String s) {
    originalRequest.removeAttribute(s);
  }

  @Override
  public Locale getLocale() {
    return originalRequest.getLocale();
  }

  @Override
  public Enumeration<Locale> getLocales() {
    return originalRequest.getLocales();
  }

  @Override
  public boolean isSecure() {
    return originalRequest.isSecure();
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String s) {
    return originalRequest.getRequestDispatcher(s);
  }

  @Override
  public String getRealPath(String s) {
    return originalRequest.getRealPath(s);
  }

  @Override
  public int getRemotePort() {
    return originalRequest.getRemotePort();
  }

  @Override
  public String getLocalName() {
    return originalRequest.getLocalName();
  }

  @Override
  public String getLocalAddr() {
    return originalRequest.getLocalAddr();
  }

  @Override
  public int getLocalPort() {
    return originalRequest.getLocalPort();
  }

  @Override
  public ServletContext getServletContext() {
    return ctx;
  }

  public void setServletContext(ServletContext newContext) {
    ctx = newContext;
  }

  @Override
  public AsyncContext startAsync() throws IllegalStateException {
    return originalRequest.startAsync();
  }

  @Override
  public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse)
    throws IllegalStateException {
    return originalRequest.startAsync(servletRequest, servletResponse);
  }

  @Override
  public boolean isAsyncStarted() {
    return originalRequest.isAsyncStarted();
  }

  @Override
  public boolean isAsyncSupported() {
    return originalRequest.isAsyncSupported();
  }

  @Override
  public AsyncContext getAsyncContext() {
    return originalRequest.getAsyncContext();
  }

  @Override
  public DispatcherType getDispatcherType() {
    return originalRequest.getDispatcherType();
  }
}
