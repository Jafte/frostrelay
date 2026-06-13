FROM golang:1.26-alpine AS build
RUN apk add --no-cache tzdata
WORKDIR /src
COPY go.mod main.go index.html ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /frostrelay .

FROM scratch
# zoneinfo — чтобы TZ влиял на имена файлов архива
COPY --from=build /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build /frostrelay /frostrelay
EXPOSE 8000
ENTRYPOINT ["/frostrelay", "-listen", ":8000", "-archive", "/archive"]
