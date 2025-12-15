go run ./cmd/ddsketchload -enable-ddsketch=false -enable-histogram=false -enable-raw=true   --endpoint localhost:4317   --insecure   --ddsketch-accuracy 0.01   --rate 100000   --workers 4
