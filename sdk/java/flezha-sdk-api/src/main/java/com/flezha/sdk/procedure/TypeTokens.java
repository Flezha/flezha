package com.flezha.sdk.procedure;

import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Recovers a {@link Procedure} implementation's type arguments from its generic
 * supertype chain, so an implementation is not charged with restating as
 * {@code Class} literals what its {@code implements Procedure<…>} clause
 * already says.
 *
 * <p>The walk substitutes type variables at every hop, so a concrete subclass
 * of a generic intermediate class resolves as well as a direct implementation.
 * An argument still generic at the concrete class is never guessed — the caller
 * gets a failure naming the class and the parameter.
 */
final class TypeTokens {

    /**
     * {@link Procedure}'s type parameters in declaration order, each paired
     * with the token method an implementation overrides to declare it
     * explicitly. {@code State} has no token method — it is read only to
     * default {@code kryoTypes()} — so it names the type parameter alone.
     */
    enum Token {
        INPUT("Input", "inputType"),
        STATE("State", null),
        OUTPUT("Output", "outputType"),
        CHANGE("ChangeT", "changeType"),
        CLOCK("ClockT", "clockType"),
        CONFIG("ConfigT", "configType");

        private final String parameter;
        private final String method;

        Token(String parameter, String method) {
            this.parameter = parameter;
            this.method = method;
        }
    }

    /** One walk per implementation class; every token read shares it. */
    private static final ClassValue<Type[]> RESOLVED = new ClassValue<>() {
        @Override
        protected Type[] computeValue(Class<?> impl) {
            Type[] found = walk(impl, Map.of());
            return found != null ? found : new Type[Token.values().length];
        }
    };

    private TypeTokens() {}

    /** The {@code token} type argument; fails by name when unresolvable. */
    static Class<?> require(Class<?> impl, Token token) {
        Class<?> resolved = find(impl, token);
        if (resolved == null) {
            throw StreamOperativeConfigurationException.unresolvedTypeToken(
                    impl.getName(), token.parameter, token.method);
        }
        return resolved;
    }

    /** The {@code token} type argument, or {@code null} when unresolvable. */
    static Class<?> find(Class<?> impl, Token token) {
        return erase(RESOLVED.get(impl)[token.ordinal()]);
    }

    /**
     * Depth-first search of {@code raw}'s supertypes for the {@code Procedure}
     * instantiation. {@code subst} binds {@code raw}'s own type variables to
     * what the caller instantiated them with.
     */
    private static Type[] walk(Class<?> raw, Map<TypeVariable<?>, Type> subst) {
        for (Type sup : supertypes(raw)) {
            Class<?> supRaw = erase(sup);
            if (supRaw == null || !Procedure.class.isAssignableFrom(supRaw)) {
                continue;
            }
            Map<TypeVariable<?>, Type> bound = bind(supRaw, sup, subst);
            if (supRaw == Procedure.class) {
                TypeVariable<?>[] vars = Procedure.class.getTypeParameters();
                Type[] args = new Type[vars.length];
                for (int i = 0; i < vars.length; i++) {
                    args[i] = bound.get(vars[i]);
                }
                return args;
            }
            Type[] found = walk(supRaw, bound);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static List<Type> supertypes(Class<?> raw) {
        List<Type> supers = new ArrayList<>(List.of(raw.getGenericInterfaces()));
        Type superclass = raw.getGenericSuperclass();
        if (superclass != null) {
            supers.add(superclass);
        }
        return supers;
    }

    /**
     * Bind {@code raw}'s type variables to the arguments {@code usage}
     * instantiates them with, resolving any that are themselves variables
     * through {@code outer}. An argument that stays a variable there is left
     * unbound — that is the unresolvable case, reported rather than guessed.
     */
    private static Map<TypeVariable<?>, Type> bind(Class<?> raw, Type usage, Map<TypeVariable<?>, Type> outer) {
        if (!(usage instanceof ParameterizedType parameterized)) {
            return Map.of();
        }
        TypeVariable<?>[] vars = raw.getTypeParameters();
        Type[] args = parameterized.getActualTypeArguments();
        Map<TypeVariable<?>, Type> bound = new HashMap<>();
        for (int i = 0; i < vars.length && i < args.length; i++) {
            Type arg = args[i];
            bound.put(vars[i], arg instanceof TypeVariable<?> variable ? outer.get(variable) : arg);
        }
        return bound;
    }

    /** The runtime class behind a type, or {@code null} for one that has none. */
    private static Class<?> erase(Type type) {
        if (type instanceof Class<?> cls) {
            return cls;
        }
        if (type instanceof ParameterizedType parameterized) {
            return erase(parameterized.getRawType());
        }
        return null;
    }
}
