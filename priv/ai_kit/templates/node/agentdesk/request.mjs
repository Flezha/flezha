const ROUTE = /^\/sessions\/([a-z][a-z0-9_-]{0,63})\/(messages|events)$/;

/**
 * One HTTP request, as the plugin hands it to the gateway. The gateway knows
 * two routes: `POST /sessions/<user>/messages`, whose body is the message, and
 * `GET /sessions/<user>/events`, the session's answers as they are written.
 */
export class Request {
  // The envelope carries more than the gateway reads.
  static decode(bytes) {
    const { method, path, body } = JSON.parse(new TextDecoder().decode(bytes));
    return new Request(method, path, body);
  }

  constructor(method, path, body) {
    this.method = method;
    this.path = path;
    this.body = body;
  }

  sends() {
    return this.#is('POST', 'messages');
  }

  listens() {
    return this.#is('GET', 'events');
  }

  user() {
    return this.#route(1);
  }

  message() {
    return { user: this.user(), text: this.body ?? '' }; // the session is named after the user
  }

  #is(verb, what) {
    return this.method === verb && this.#route(2) === what;
  }

  #route(part) {
    return ROUTE.exec(this.path)?.[part] ?? ''; // '' when the path is not a route
  }
}
