package com.flezha.sdk.processor;

import com.flezha.sdk.authoring.Procedure;
import com.flezha.sdk.authoring.State;
import com.flezha.sdk.authoring.StreamOperative;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

/**
 * Generates {@code Procedure} / {@code StreamOperative} implementations from the
 * {@code @Procedure} / {@code @State} / {@code @StreamOperative} authoring annotations,
 * mirroring the Rust SDK macros. See {@code docs/sdk/authoring_contract.md}.
 *
 * <p>Per round: {@code @State} classes generate a {@code <Name>Change}
 * (field-granular variants + {@code apply} + {@code kryoTypes}); {@code @Procedure}
 * classes generate a {@code <Name>Procedure} (and, with a {@code sink}, a
 * single-procedure {@code <Name>StreamOperative}); {@code @StreamOperative} classes are collected. On
 * the final round a {@code com.flezha.generated.GeneratedStreamOperatives.defineAll(FlezhaRunner)}
 * aggregator is emitted so authors register every sugared operative with one call.
 *
 * <p>This class owns the rounds and the two sinks every generator writes through
 * ({@link #write} and {@link #error}); {@link StateGenerator} and
 * {@link ProcedureGenerator} own what is generated.
 */
@SupportedAnnotationTypes({
    "com.flezha.sdk.authoring.Procedure",
    "com.flezha.sdk.authoring.State",
    "com.flezha.sdk.authoring.StreamOperative"
})
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public final class FlezhaProcessor extends AbstractProcessor {

    private Elements elements;
    private Types types;
    private Filer filer;
    private StateGenerator states;
    private ProcedureGenerator procedures;

    /** Fully-qualified operative class names collected across rounds. */
    private final List<String> operativeFqns = new ArrayList<>();

    private boolean aggregatorEmitted = false;

    @Override
    public synchronized void init(javax.annotation.processing.ProcessingEnvironment env) {
        super.init(env);
        this.elements = env.getElementUtils();
        this.types = env.getTypeUtils();
        this.filer = env.getFiler();
        TypeMirrors mirrors = new TypeMirrors(elements, types);
        this.states = new StateGenerator(this, mirrors);
        this.procedures = new ProcedureGenerator(this, mirrors);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element e : roundEnv.getElementsAnnotatedWith(State.class)) {
            states.generate((TypeElement) e);
        }
        for (Element e : roundEnv.getElementsAnnotatedWith(Procedure.class)) {
            String operative = procedures.generate((TypeElement) e);
            if (operative != null) {
                operativeFqns.add(operative);
            }
        }
        for (Element e : roundEnv.getElementsAnnotatedWith(StreamOperative.class)) {
            if (implementsStreamOperative((TypeElement) e)) {
                operativeFqns.add(((TypeElement) e).getQualifiedName().toString());
            } else {
                error(e, "@StreamOperative requires the class to implement com.flezha.sdk.operative.StreamOperative");
            }
        }

        if (roundEnv.processingOver() && !aggregatorEmitted && !operativeFqns.isEmpty()) {
            emitAggregator();
            aggregatorEmitted = true;
        }
        return false;
    }

    private void emitAggregator() {
        StringBuilder b = source("com.flezha.generated");
        b.append("import com.flezha.sdk.FlezhaRunner;\n\n");
        b.append("/** Generated aggregator: defines every sugared operative on the runner with one call. */\n");
        b.append("public final class GeneratedStreamOperatives {\n");
        b.append("    private GeneratedStreamOperatives() {}\n\n");
        b.append("    public static void defineAll(FlezhaRunner runner) {\n");
        for (String fqn : operativeFqns) {
            b.append("        runner.defineStream(").append(fqn).append(".class);\n");
        }
        b.append("    }\n}\n");
        write("com.flezha.generated.GeneratedStreamOperatives", b.toString());
    }

    private boolean implementsStreamOperative(TypeElement t) {
        TypeElement operative = elements.getTypeElement("com.flezha.sdk.operative.StreamOperative");
        if (operative == null) {
            return false;
        }
        for (TypeMirror itf : t.getInterfaces()) {
            if (types.isSameType(types.erasure(itf), types.erasure(operative.asType()))) {
                return true;
            }
        }
        return false;
    }

    /** Opens a generated source with its package declaration; the default package gets none. */
    static StringBuilder source(String pkg) {
        StringBuilder b = new StringBuilder();
        if (!pkg.isEmpty()) {
            b.append("package ").append(pkg).append(";\n\n");
        }
        return b;
    }

    void write(String fqn, String content, Element... origin) {
        try {
            var file = filer.createSourceFile(fqn, origin);
            try (Writer w = file.openWriter()) {
                w.write(content);
            }
        } catch (IOException ex) {
            processingEnv
                    .getMessager()
                    .printMessage(Diagnostic.Kind.ERROR, "FlezhaProcessor: failed to write " + fqn + ": " + ex);
        }
    }

    void error(Element e, String msg) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, msg, e);
    }
}
