<?php

declare(strict_types=1);

namespace Flezha\Examples\Agentdesk;

/**
 * One HTTP request, as the plugin hands it to the gateway. The gateway knows two routes:
 * `POST /sessions/<user>/messages`, whose body is the message, and `GET /sessions/<user>/events`,
 * the session's answers as they are written.
 */
final readonly class Request
{
    private const ROUTE = '#^/sessions/([a-z][a-z0-9_-]{0,63})/(messages|events)$#';

    private function __construct(private string $method, private string $path, private ?string $body)
    {
    }

    // The envelope carries more than the gateway reads.
    public static function decode(string $json): self
    {
        $envelope = json_decode($json, true, flags: JSON_THROW_ON_ERROR);

        return new self($envelope['method'], $envelope['path'], $envelope['body'] ?? null);
    }

    public function sends(): bool
    {
        return $this->is('POST', 'messages');
    }

    public function listens(): bool
    {
        return $this->is('GET', 'events');
    }

    public function user(): string
    {
        return $this->route(1);
    }

    /**
     * A message for the user's session, as the gateway sends it: the session is named after the user.
     *
     * @return array{user: string, text: string}
     */
    public function message(): array
    {
        return ['user' => $this->user(), 'text' => $this->body ?? ''];
    }

    private function is(string $verb, string $what): bool
    {
        return $this->method === $verb && $this->route(2) === $what;
    }

    private function route(int $part): string        // "" when the path is not a route
    {
        return preg_match(self::ROUTE, $this->path, $route) === 1 ? $route[$part] : '';
    }
}
