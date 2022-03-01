# Rest API

Nessie's REST APIs are how all applications interact with Nessie. The APIs are specified
according to the openapi v3 standard and are available when running the server by going
to [localhost:19120/q/openapi](http://localhost:19120/q/openapi). You can also peruse the set of operations our APIs support
by going to [SwaggerHub](https://app.swaggerhub.com/apis/projectnessie/nessie).

If you are working in development, our Quarkus server will automatically start with
the swagger-ui for experimentation. You can find that at [localhost:19120/q/swagger-ui/](http://localhost:19120/q/swagger-ui/)
