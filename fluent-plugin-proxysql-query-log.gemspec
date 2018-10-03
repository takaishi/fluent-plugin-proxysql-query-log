lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name    = "fluent-plugin-proxysql-query-log"
  spec.version = "0.4.2"
  spec.authors = ["r_takaishi"]
  spec.email   = ["ryo.takaishi.0@gmail.com"]

  spec.summary       = 'Input plugin to read from ProxySQL query log.'
  spec.description   = spec.summary
  spec.homepage      = 'https://github.com/takaishi/fluent-plugin-proxysql-query-log'
  spec.license       = "Apache-2.0"

  test_files, files  = `git ls-files -z`.split("\x0").partition do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.files         = files
  spec.executables   = files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = test_files
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.14"
  spec.add_development_dependency "rake", "~> 12.0"
  spec.add_development_dependency "test-unit", "~> 3.0"
  spec.add_development_dependency "fluent-plugin-systemd"
  spec.add_runtime_dependency "fluentd", [">= 0.14.2", "< 2"]
  spec.add_dependency 'cool.io'
  spec.add_dependency 'proxysql_query_log-parser', "0.0.4"
end
