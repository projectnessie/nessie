/*
 * Copyright (C) 2020 Dremio
 *
 *             Licensed under the Apache License, Version 2.0 (the "License");
 *             you may not use this file except in compliance with the License.
 *             You may obtain a copy of the License at
 *
 *             http://www.apache.org/licenses/LICENSE-2.0
 *
 *             Unless required by applicable law or agreed to in writing, software
 *             distributed under the License is distributed on an "AS IS" BASIS,
 *             WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *             See the License for the specific language governing permissions and
 *             limitations under the License.
 */
package com.dremio.iceberg;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.CharBuffer;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ReadListener;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verbose Access log filter to help debugging
 */
final class AccessLogFilter implements Filter {
  private static final Logger logger = LoggerFactory.getLogger(AccessLogFilter.class);

  private long nextReqId = 0;

  private PrintWriter docLog;

  private Set<String> printableContentTypes = new HashSet<>(Arrays.asList(
      "text/html; charset=UTF-8",
      "application/json",
      "text/plain"));

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  private void log(String message) {
    logger.debug("{}", message);
    if (docLog != null) {
      docLog.println(message);
    }
  }

  String bodyToString(String contentType, StringWriter body) {
    return
        printableContentTypes.contains(contentType) ?
            body.toString() :
            contentType + " length: " + body.getBuffer().length();
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    long reqId = nextReqId++;
    HttpServletRequest req = (HttpServletRequest) request;
    HttpServletResponse resp = (HttpServletResponse) response;

    StringBuffer requestURL = req.getRequestURL();
    String queryString = req.getQueryString();
    final String fullUrl;
    if (queryString == null) {
      fullUrl = requestURL.toString();
    } else {
      fullUrl = requestURL.append('?').append(queryString).toString();
    }


    log(String.format("%d: %s %s", reqId, req.getMethod(), fullUrl));
    String referer = req.getHeader("Referer");
    if (referer != null) {
      log(String.format("%d: referer=%s", reqId, referer));
    }
    StringWriter reqBody = new StringWriter();
    StringWriter respBody = new StringWriter();
    long t0 = System.currentTimeMillis();
    chain.doFilter(new HTTPRequestWrapper(req, reqBody), new HTTPResponseWrapper(resp, respBody));
    long t1 = System.currentTimeMillis();
    if (reqBody.toString().trim().length() > 0) {
      log(reqId + ": request body:\n" + prefixLines(reqId + ": => ", bodyToString(req.getContentType(), reqBody)));
    }
    log(String.format(
        "%d: Status %s, Content-type: %s, returned in %dms", reqId, resp.getStatus(), resp.getContentType(), t1 - t0));
    if (respBody.getBuffer().length() > 0) {
      log(reqId + ": response body:\n" + prefixLines(reqId + ": <= ", bodyToString(resp.getContentType(), respBody)));

    }
  }

  private String prefixLines(String prefix, String string) {
    String[] lines = string.toString().split("\n");
    StringBuilder sb = new StringBuilder();
    for (String line : lines) {
      sb.append(prefix).append(line).append("\n");
    }
    return sb.toString();
  }

  @Override
  public void destroy() {
  }

  private static class HTTPResponseWrapper implements HttpServletResponse {
    private final HttpServletResponse httpServletResponse;
    private StringWriter respBody;

    HTTPResponseWrapper(HttpServletResponse httpServletResponse, StringWriter respBody) {
      this.httpServletResponse = httpServletResponse;
      this.respBody = respBody;
    }

    @Override
    public void addCookie(Cookie cookie) {
      httpServletResponse.addCookie(cookie);
    }

    @Override
    public boolean containsHeader(String name) {
      return httpServletResponse.containsHeader(name);
    }

    @Override
    public String encodeURL(String url) {
      return httpServletResponse.encodeURL(url);
    }

    @Override
    public String getCharacterEncoding() {
      return httpServletResponse.getCharacterEncoding();
    }

    @Override
    public String encodeRedirectURL(String url) {
      return httpServletResponse.encodeRedirectURL(url);
    }

    @Override
    public String encodeUrl(String url) {
      return null;
    }

    @Override
    public String encodeRedirectUrl(String url) {
      return null;
    }

    @Override
    public String getContentType() {
      return httpServletResponse.getContentType();
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {

      final ServletOutputStream outputStream = httpServletResponse.getOutputStream();
      return new ServletOutputStream() {

        @Override
        public void write(int b) throws IOException {
          respBody.write(b);
          outputStream.write(b);
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
          outputStream.setWriteListener(writeListener);
        }

        @Override
        public boolean isReady() {
          return outputStream.isReady();
        }
      };
    }

    @Override
    public void sendError(int sc, String msg) throws IOException {
      httpServletResponse.sendError(sc, msg);
    }

    @Override
    public void sendError(int sc) throws IOException {
      httpServletResponse.sendError(sc);
    }

    @Override
    public PrintWriter getWriter() throws IOException {
      return new PrintWriter(new WrappedWriter(httpServletResponse.getWriter(), respBody));
    }

    @Override
    public void setCharacterEncoding(String charset) {
      httpServletResponse.setCharacterEncoding(charset);
    }

    @Override
    public void sendRedirect(String location) throws IOException {
      httpServletResponse.sendRedirect(location);
    }

    @Override
    public void setContentLength(int len) {
      httpServletResponse.setContentLength(len);
    }

    @Override
    public void setContentLengthLong(long len) {
      httpServletResponse.setContentLengthLong(len);
    }

    @Override
    public void setDateHeader(String name, long date) {
      httpServletResponse.setDateHeader(name, date);
    }

    @Override
    public void setContentType(String type) {
      httpServletResponse.setContentType(type);
    }

    @Override
    public void addDateHeader(String name, long date) {
      httpServletResponse.addDateHeader(name, date);
    }

    @Override
    public void setHeader(String name, String value) {
      httpServletResponse.setHeader(name, value);
    }

    @Override
    public void setBufferSize(int size) {
      httpServletResponse.setBufferSize(size);
    }

    @Override
    public void addHeader(String name, String value) {
      httpServletResponse.addHeader(name, value);
    }

    @Override
    public void setIntHeader(String name, int value) {
      httpServletResponse.setIntHeader(name, value);
    }

    @Override
    public int getBufferSize() {
      return httpServletResponse.getBufferSize();
    }

    @Override
    public void addIntHeader(String name, int value) {
      httpServletResponse.addIntHeader(name, value);
    }

    @Override
    public void flushBuffer() throws IOException {
      httpServletResponse.flushBuffer();
    }

    @Override
    public void setStatus(int sc) {
      httpServletResponse.setStatus(sc);
    }

    @Override
    public void setStatus(int sc, String sm) {

    }

    @Override
    public void resetBuffer() {
      httpServletResponse.resetBuffer();
    }

    @Override
    public boolean isCommitted() {
      return httpServletResponse.isCommitted();
    }

    @Override
    public void reset() {
      httpServletResponse.reset();
    }

    @Override
    public int getStatus() {
      return httpServletResponse.getStatus();
    }

    @Override
    public String getHeader(String name) {
      return httpServletResponse.getHeader(name);
    }

    @Override
    public void setLocale(Locale loc) {
      httpServletResponse.setLocale(loc);
    }

    @Override
    public Collection<String> getHeaders(String name) {
      return httpServletResponse.getHeaders(name);
    }

    @Override
    public Collection<String> getHeaderNames() {
      return httpServletResponse.getHeaderNames();
    }

    @Override
    public Locale getLocale() {
      return httpServletResponse.getLocale();
    }

  }


  private static class HTTPRequestWrapper implements HttpServletRequest {
    private final HttpServletRequest httpServletRequest;
    private StringWriter reqBody;

    HTTPRequestWrapper(HttpServletRequest httpServletRequest, StringWriter reqBody) {
      this.httpServletRequest = httpServletRequest;
      this.reqBody = reqBody;
    }

    @Override
    public Object getAttribute(String name) {
      return httpServletRequest.getAttribute(name);
    }

    @Override
    public String getAuthType() {
      return httpServletRequest.getAuthType();
    }

    @Override
    public Cookie[] getCookies() {
      return httpServletRequest.getCookies();
    }

    @Override
    public Enumeration<String> getAttributeNames() {
      return httpServletRequest.getAttributeNames();
    }

    @Override
    public long getDateHeader(String name) {
      return httpServletRequest.getDateHeader(name);
    }

    @Override
    public String getCharacterEncoding() {
      return httpServletRequest.getCharacterEncoding();
    }

    @Override
    public void setCharacterEncoding(String env) throws UnsupportedEncodingException {
      httpServletRequest.setCharacterEncoding(env);
    }

    @Override
    public int getContentLength() {
      return httpServletRequest.getContentLength();
    }

    @Override
    public String getHeader(String name) {
      return httpServletRequest.getHeader(name);
    }

    @Override
    public long getContentLengthLong() {
      return httpServletRequest.getContentLengthLong();
    }

    @Override
    public Enumeration<String> getHeaders(String name) {
      return httpServletRequest.getHeaders(name);
    }

    @Override
    public String getContentType() {
      return httpServletRequest.getContentType();
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
      final ServletInputStream inputStream = httpServletRequest.getInputStream();
      return new ServletInputStream() {

        @Override
        public int read() throws IOException {
          int bRead = inputStream.read();
          if (bRead != -1) {
            reqBody.write(bRead);
          }
          return bRead;
        }

        @Override
        public void setReadListener(ReadListener readListener) {
          inputStream.setReadListener(readListener);
        }

        @Override
        public boolean isReady() {
          return inputStream.isReady();
        }

        @Override
        public boolean isFinished() {
          return inputStream.isFinished();
        }
      };
    }

    @Override
    public String getParameter(String name) {
      return httpServletRequest.getParameter(name);
    }

    @Override
    public Enumeration<String> getHeaderNames() {
      return httpServletRequest.getHeaderNames();
    }

    @Override
    public int getIntHeader(String name) {
      return httpServletRequest.getIntHeader(name);
    }

    @Override
    public Enumeration<String> getParameterNames() {
      return httpServletRequest.getParameterNames();
    }

    @Override
    public String getMethod() {
      return httpServletRequest.getMethod();
    }

    @Override
    public String[] getParameterValues(String name) {
      return httpServletRequest.getParameterValues(name);
    }

    @Override
    public String getPathInfo() {
      return httpServletRequest.getPathInfo();
    }

    @Override
    public Map<String, String[]> getParameterMap() {
      return httpServletRequest.getParameterMap();
    }

    @Override
    public String getPathTranslated() {
      return httpServletRequest.getPathTranslated();
    }

    @Override
    public String getProtocol() {
      return httpServletRequest.getProtocol();
    }

    @Override
    public String getScheme() {
      return httpServletRequest.getScheme();
    }

    @Override
    public String getContextPath() {
      return httpServletRequest.getContextPath();
    }

    @Override
    public String getServerName() {
      return httpServletRequest.getServerName();
    }

    @Override
    public int getServerPort() {
      return httpServletRequest.getServerPort();
    }

    @Override
    public BufferedReader getReader() throws IOException {
      return new BufferedReader(new WrappedReader(httpServletRequest.getReader(), reqBody));
    }

    @Override
    public String getQueryString() {
      return httpServletRequest.getQueryString();
    }

    @Override
    public String getRemoteUser() {
      return httpServletRequest.getRemoteUser();
    }

    @Override
    public String getRemoteAddr() {
      return httpServletRequest.getRemoteAddr();
    }

    @Override
    public String getRemoteHost() {
      return httpServletRequest.getRemoteHost();
    }

    @Override
    public boolean isUserInRole(String role) {
      return httpServletRequest.isUserInRole(role);
    }

    @Override
    public void setAttribute(String name, Object o) {
      httpServletRequest.setAttribute(name, o);
    }

    @Override
    public Principal getUserPrincipal() {
      return httpServletRequest.getUserPrincipal();
    }

    @Override
    public void removeAttribute(String name) {
      httpServletRequest.removeAttribute(name);
    }

    @Override
    public String getRequestedSessionId() {
      return httpServletRequest.getRequestedSessionId();
    }

    @Override
    public Locale getLocale() {
      return httpServletRequest.getLocale();
    }

    @Override
    public String getRequestURI() {
      return httpServletRequest.getRequestURI();
    }

    @Override
    public Enumeration<Locale> getLocales() {
      return httpServletRequest.getLocales();
    }

    @Override
    public boolean isSecure() {
      return httpServletRequest.isSecure();
    }

    @Override
    public StringBuffer getRequestURL() {
      return httpServletRequest.getRequestURL();
    }

    @Override
    public RequestDispatcher getRequestDispatcher(String path) {
      return httpServletRequest.getRequestDispatcher(path);
    }

    @Override
    public String getServletPath() {
      return httpServletRequest.getServletPath();
    }

    /**
     * @deprecated see upstream
     */
    @Override
    @Deprecated
    public String getRealPath(String path) {
      return httpServletRequest.getRealPath(path);
    }

    @Override
    public HttpSession getSession(boolean create) {
      return httpServletRequest.getSession(create);
    }

    @Override
    public HttpSession getSession() {
      return httpServletRequest.getSession();
    }

    @Override
    public int getRemotePort() {
      return httpServletRequest.getRemotePort();
    }

    @Override
    public String getLocalName() {
      return httpServletRequest.getLocalName();
    }

    @Override
    public String getLocalAddr() {
      return httpServletRequest.getLocalAddr();
    }

    @Override
    public int getLocalPort() {
      return httpServletRequest.getLocalPort();
    }

    @Override
    public ServletContext getServletContext() {
      return httpServletRequest.getServletContext();
    }

    @Override
    public AsyncContext startAsync() throws IllegalStateException {
      return httpServletRequest.startAsync();
    }

    @Override
    public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse)
        throws IllegalStateException {
      return httpServletRequest.startAsync(servletRequest, servletResponse);
    }

    @Override
    public String changeSessionId() {
      return httpServletRequest.changeSessionId();
    }

    @Override
    public boolean isRequestedSessionIdValid() {
      return httpServletRequest.isRequestedSessionIdValid();
    }

    @Override
    public boolean isRequestedSessionIdFromCookie() {
      return httpServletRequest.isRequestedSessionIdFromCookie();
    }

    @Override
    public boolean isRequestedSessionIdFromURL() {
      return httpServletRequest.isRequestedSessionIdFromURL();
    }

    /**
     * @deprecated see upstream
     */
    @Override
    @Deprecated
    public boolean isRequestedSessionIdFromUrl() {
      return httpServletRequest.isRequestedSessionIdFromUrl();
    }

    @Override
    public boolean authenticate(HttpServletResponse response) throws IOException, ServletException {
      return httpServletRequest.authenticate(response);
    }

    @Override
    public void login(String username, String password) throws ServletException {
      httpServletRequest.login(username, password);
    }

    @Override
    public void logout() throws ServletException {
      httpServletRequest.logout();
    }

    @Override
    public Collection<Part> getParts() throws IOException, ServletException {
      return httpServletRequest.getParts();
    }

    @Override
    public boolean isAsyncStarted() {
      return httpServletRequest.isAsyncStarted();
    }

    @Override
    public boolean isAsyncSupported() {
      return httpServletRequest.isAsyncSupported();
    }

    @Override
    public Part getPart(String name) throws IOException, ServletException {
      return httpServletRequest.getPart(name);
    }

    @Override
    public AsyncContext getAsyncContext() {
      return httpServletRequest.getAsyncContext();
    }

    @Override
    public DispatcherType getDispatcherType() {
      return httpServletRequest.getDispatcherType();
    }

    @Override
    public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass) throws IOException, ServletException {
      return httpServletRequest.upgrade(handlerClass);
    }

  }

  public static class WrappedReader extends Reader {

    private Reader delegate;
    private StringWriter reqBody;

    WrappedReader(Reader delegate, StringWriter reqBody) {
      this.delegate = delegate;
      this.reqBody = reqBody;
    }

    @Override
    public int read(CharBuffer target) throws IOException {
      int len = target.remaining();
      char[] cbuf = new char[len];
      int num = read(cbuf, 0, len);
      if (num > 0) {
        target.put(cbuf, 0, num);
      }
      return num;
    }

    @Override
    public int read(char[] cbuf) throws IOException {
      return read(cbuf, 0, cbuf.length);
    }

    @Override
    public int read() throws IOException {
      int read = delegate.read();
      reqBody.write(read);
      return read;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      int num = delegate.read(cbuf, off, len);
      for (int i = 0; i < num; i++) {
        char chr = cbuf[off + i];
        reqBody.write(chr);
      }
      return num;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

  }

  public static class WrappedWriter extends Writer {

    private PrintWriter printWriter;
    private StringWriter respBody;

    WrappedWriter(PrintWriter printWriter, StringWriter respBody) {
      this.printWriter = printWriter;
      this.respBody = respBody;
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
      respBody.write(cbuf, off, len);
      printWriter.write(cbuf, off, len);
    }

    @Override
    public void flush() throws IOException {
      printWriter.flush();
    }

    @Override
    public void close() throws IOException {
      printWriter.close();
    }

  }

  public void stopLoggingToFile() {
    if (this.docLog != null) {
      this.docLog.flush();
      this.docLog = null;
    }
  }

  public void startLoggingToFile(PrintWriter dLog) {
    this.docLog = dLog;
  }
}
