create table ml as

with t2 as 
   (select * from players 
    UNION
    select distinct exp+1, name, 2017,age+1, Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null
   from players where year=2016
   )

select t2.name, t2.year, t2.age, t2.exp, t2.team, t2.gp, t2.gs, t2.mp,
    coalesce (t1.ztot0,0) as ztot0, t2.ztot, coalesce (t1.ztotdiff,0) as ztotdiff, 
    coalesce (t1.ntot0,0) as ntot0, t2.ntot, coalesce (t1.ntotdiff,0) as ntotdiff,
    coalesce (t1.zfg0,0) as zfg0, t2.zfg, coalesce (t1.zfgdiff,0) as zfgdiff,
    coalesce (t1.zft0,0) as zft0, t2.zft, coalesce (t1.zftdiff,0) as zftdiff,
    coalesce (t1.z3p0,0) as z3p0, t2.z3p, coalesce (t1.z3pdiff,0) as z3pdiff,
    coalesce (t1.ztrb0,0) as ztrb0, t2.ztrb, coalesce (t1.zdtrbiff,0) as ztrbdiff,
    coalesce (t1.zast0,0) as zast0, t2.zast, coalesce (t1.zastdiff,0) as zastdiff,
    coalesce (t1.zstl0,0) as zstl0, t2.zstl, coalesce (t1.zstldiff,0) as zstldiff,
    coalesce (t1.zblk0,0) as zblk0, t2.zblk, coalesce (t1.zblkdiff,0) as zblkdiff,
    coalesce (t1.ztov0,0) as ztov0, t2.ztov, coalesce (t1.ztovdiff,0) as ztovdiff,
    coalesce (t1.zpts0,0) as zpts0, t2.zpts, coalesce (t1.zptsdiff,0) as zptsdiff
  from t2
  join (
    select p2.name, p2.year, p2.age, p2.exp, p2.team, p2.gp, p2.gs, p2.mp,
    p1.ztot as ztot0, p2.ztot, (p2.zTot-p1.zTot) as ztotdiff, 
    p1.ntot as ntot0, p2.ntot, (p2.nTot-p1.nTot) as ntotdiff,
    p1.zfg as zfg0, p2.zfg, (p2.zfg-p1.zfg) as zfgdiff,
    p1.zft as zft0, p2.zft, (p2.zft-p1.zft) as zftdiff,
    p1.z3p as z3p0, p2.z3p, (p2.z3p-p1.z3p) as z3pdiff,
    p1.ztrb as ztrb0, p2.ztrb, (p2.ztrb-p1.ztrb) as zdtrbiff,
    p1.zast as zast0, p2.zast, (p2.zast-p1.zast) as zastdiff,
    p1.zstl as zstl0, p2.zstl, (p2.zstl-p1.zstl) as zstldiff,
    p1.zblk as zblk0, p2.zblk, (p2.zblk-p1.zblk) as zblkdiff,
    p1.ztov as ztov0, p2.ztov, (p2.ztov-p1.ztov) as ztovdiff,
    p1.zpts as zpts0, p2.zpts, (p2.zpts-p1.zpts) as zptsdiff
    from t2 as p1 
    join t2 as p2 
    where p1.name=p2.name 
    and p1.year=p2.year-1) as t1 
    on t2.name=t1.name 
    and t2.year=t1.year
