// 목차 현재 위치 표시: 화면 위 30% 선을 지난 마지막 제목을 현재 위치로 본다
(() => {
  const links = [...document.querySelectorAll('.toc a')];
  const targets = links.map(a => document.querySelector(a.getAttribute('href'))).filter(Boolean);
  if (!targets.length) return;
  let ticking = false;
  const update = () => {
    ticking = false;
    const line = innerHeight * 0.3;
    let idx = -1;
    targets.forEach((t, i) => { if (t.getBoundingClientRect().top <= line) idx = i; });
    links.forEach((a, i) => i === idx ? a.setAttribute('aria-current', 'true') : a.removeAttribute('aria-current'));
  };
  addEventListener('scroll', () => { if (!ticking) { ticking = true; requestAnimationFrame(update); } }, { passive: true });
  update();
})();
