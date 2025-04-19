.PHONY: generate test bump

generate:
	go generate ./...

test:
	go test -v ./... 

bump:
	@echo "Current version:"
	@git describe --tags --abbrev=0 2>/dev/null || echo "No tags found, starting with v0.0.0"
	$(eval CURRENT_VERSION := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"))
	$(eval CURRENT_VERSION_NUM := $(shell echo $(CURRENT_VERSION) | sed 's/v//'))
	$(eval MAJOR := $(shell echo $(CURRENT_VERSION_NUM) | cut -d. -f1))
	$(eval MINOR := $(shell echo $(CURRENT_VERSION_NUM) | cut -d. -f2))
	$(eval PATCH := $(shell echo $(CURRENT_VERSION_NUM) | cut -d. -f3))
	$(eval NEW_PATCH := $(shell echo $$(($(PATCH) + 1))))
	$(eval NEW_VERSION := v$(MAJOR).$(MINOR).$(NEW_PATCH))
	$(eval CURRENT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD))
	@echo "Bumping to $(NEW_VERSION)"
	@git commit --allow-empty -m "bump: $(NEW_VERSION)"
	@git tag -a $(NEW_VERSION) -m "bump: $(NEW_VERSION)"
	@git push --set-upstream origin $(CURRENT_BRANCH) || git push
	@git push origin $(NEW_VERSION)
	@echo "Successfully bumped version to $(NEW_VERSION), committed changes, and pushed tag" 