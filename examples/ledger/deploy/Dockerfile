FROM nginx:1.30.6-alpine

COPY nginx.conf /etc/nginx/nginx.conf

USER 101:101
EXPOSE 8080
STOPSIGNAL SIGQUIT

ENTRYPOINT ["nginx"]
CMD ["-g", "daemon off;"]
