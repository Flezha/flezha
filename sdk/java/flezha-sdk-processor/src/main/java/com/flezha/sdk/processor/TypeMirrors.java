package com.flezha.sdk.processor;

import java.util.List;
import javax.lang.model.element.Element;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

/**
 * The {@code javax.lang.model} questions the generators ask: is this type a
 * {@code Map} / {@code List} / {@code Deque} / {@code Optional}, what does it
 * hold, which package is a declaration in, and how a string is spelled as a
 * literal. Nothing here knows what is being generated.
 */
final class TypeMirrors {

    private final Elements elements;
    private final Types types;

    TypeMirrors(Elements elements, Types types) {
        this.elements = elements;
        this.types = types;
    }

    /** The {@code K} and {@code V} of a {@code Map<K, V>}, or {@code null} for anything else. */
    TypeMirror[] mapKV(TypeMirror ty) {
        List<? extends TypeMirror> ta = arguments(ty, "java.util.Map", 2);
        return ta == null ? null : new TypeMirror[] {ta.get(0), ta.get(1)};
    }

    /** The {@code E} of a {@code List<E>}, or {@code null}. */
    TypeMirror listElement(TypeMirror ty) {
        return onlyArgument(ty, "java.util.List");
    }

    /** The {@code E} of a {@code Deque<E>}, or {@code null}. */
    TypeMirror dequeElement(TypeMirror ty) {
        return onlyArgument(ty, "java.util.Deque");
    }

    boolean isOptional(TypeMirror ty) {
        return erasesTo(ty, "java.util.Optional");
    }

    /** The {@code O} of an {@code Optional<O>}, or {@code null} when it is raw. */
    TypeMirror optionalElement(TypeMirror ty) {
        return onlyArgument(ty, "java.util.Optional");
    }

    /** Whether the erasure of {@code ty} is exactly {@code fqn}. */
    boolean erasesTo(TypeMirror ty, String fqn) {
        return ty instanceof DeclaredType dt && types.erasure(dt).toString().equals(fqn);
    }

    /** The type arguments of a declared type; empty for everything else. */
    List<? extends TypeMirror> typeArguments(TypeMirror ty) {
        return ty instanceof DeclaredType dt ? dt.getTypeArguments() : List.of();
    }

    String packageOf(Element e) {
        return elements.getPackageOf(e).getQualifiedName().toString();
    }

    /** {@code value} as a Java string literal, quotes and escapes included. */
    String literal(String value) {
        return elements.getConstantExpression(value);
    }

    /** The fully-qualified name a generated type gets in {@code pkg}, which may be the default package. */
    static String qualify(String pkg, String simpleName) {
        return pkg.isEmpty() ? simpleName : pkg + "." + simpleName;
    }

    /** {@code java.util.List<String>} to {@code java.util.List.class}: generics are erased for a class literal. */
    static String classLit(String typeStr) {
        int lt = typeStr.indexOf('<');
        return (lt >= 0 ? typeStr.substring(0, lt) : typeStr) + ".class";
    }

    private TypeMirror onlyArgument(TypeMirror ty, String erasedFqn) {
        List<? extends TypeMirror> ta = arguments(ty, erasedFqn, 1);
        return ta == null ? null : ta.get(0);
    }

    private List<? extends TypeMirror> arguments(TypeMirror ty, String erasedFqn, int arity) {
        List<? extends TypeMirror> ta = erasesTo(ty, erasedFqn) ? typeArguments(ty) : List.of();
        return ta.size() == arity ? ta : null;
    }
}
