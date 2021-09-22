namespace :app do

  task :test do
  end

  task :build do
    sh "./build.sh"
    archive "mxshortserver", gnu_tar: true do
      add "target"
      add "node_modules"
      add_bundle
    end
  end

  task "deploy:git" do
    sh "./build.sh"
    sh "sv rr"
  end

  task "deploy:archive" do
    sh "./remove.sh"
    sh "sv rr"
    sh "./add.sh"
  end

  task :reload do
    sh "./remove.sh"
    sh "sv rr"
    sh "./add.sh"
  end

  task :reopen_logs do
    sh "sv rr"
    sh "sv reopen_logs"
  end

  task :stop do
    sh "sv stop"
  end

end
