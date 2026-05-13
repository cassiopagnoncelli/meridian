
From console

```
> const { Meridian } = await import("meridian");
> const meridian = await Meridian.open();
> const ip = meridian.ip("2001:1284:f508:535:3dd8:5f13:1dac:ede4");
> ip
{
  source: 'maxmind',
  ip: '2001:1284:f508:535:3dd8:5f13:1dac:ede4',
  city: {
    name: 'Curitiba',
  ...
}
```
