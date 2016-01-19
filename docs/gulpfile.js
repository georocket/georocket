var asciidoctor = require("gulp-asciidoctor");
var del = require("del");
var gulp = require("gulp");
var livereload = require("gulp-livereload");
var path = require("path");

var paths = {
  asciidoctor_js: "node_modules/asciidoctor.js/dist",
  build: "build",
  src: "src",
  images: "src/images"
};

gulp.task("css", function() {
  return gulp.src(path.join(paths.asciidoctor_js, "css", "asciidoctor.css"))
    .pipe(gulp.dest(paths.build));
});

gulp.task("images", function() {
  return gulp.src(path.join(paths.images, "**", "*"))
    .pipe(gulp.dest(path.join(paths.build, "images")));
});

gulp.task("build", ["css", "images"], function() {
  return gulp.src(path.join(paths.src, "**", "*.adoc"))
    .pipe(asciidoctor({
      base_dir: paths.src,
      doctype:'article'
    }))
    .pipe(gulp.dest(paths.build))
    .pipe(livereload());
});

gulp.task("watch", ["build"], function() {
  livereload.listen();
  return gulp.watch([path.join(paths.src, "**", "*.adoc")], ["build"]);
});

gulp.task("clean", function(cb) {
  del(["build"], cb);
});

gulp.task("default", ["build"]);
